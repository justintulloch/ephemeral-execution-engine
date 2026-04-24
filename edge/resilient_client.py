"""
EMBER Resilient Routing Client — Abort-Early + Circuit Breaker
"""

import os
import sys
import time
import logging
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import grpc

import ember_pb2
import ember_pb2_grpc
from core.constants import ROUTING_TIMEOUT_MS
from edge.circuit_breaker import CircuitBreaker, CircuitState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ember.resilient_routing")

DEFAULT_TARGET = "localhost:50051"


class FallbackReason(Enum):
    """Why a request fell back to local execution."""

    NONE = "none"
    CIRCUIT_OPEN = "circuit_open"
    TIMEOUT = "timeout"
    ERROR = "error"
    PARTITION = "partition"


@dataclass
class RoutingDecision:
    """The result of a routing attempt."""

    route_to_chicago: bool
    fallback_to_local: bool
    fallback_reason: FallbackReason
    rtt_ms: float
    request_id: str
    circuit_state: CircuitState
    response: ember_pb2.RouteResponse | None = None


class ResilientRoutingClient:
    """
    Routing client with Abort-Early discipline and Circuit Breaker.

    Decision flow:
      1. Check circuit breaker — if OPEN, skip Chicago entirely (zero overhead)
      2. Send RouteRequest with strict 20ms timeout
      3. If success → record_success, return routing decision
      4. If timeout/error → record_failure, return LocalFallback
      5. After 3 failures → breaker trips to OPEN
    """

    def __init__(
        self,
        target: str = DEFAULT_TARGET,
        circuit_breaker: CircuitBreaker | None = None,
    ) -> None:
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = ember_pb2_grpc.RegionalRouterStub(self.channel)
        self.timeout_s = ROUTING_TIMEOUT_MS / 1000.0
        self.circuit_breaker = circuit_breaker or CircuitBreaker()

        # Telemetry counters
        self.total_requests = 0
        self.total_successes = 0
        self.total_timeouts = 0
        self.total_errors = 0
        self.total_circuit_skips = 0

    def request_route(self, request_id: str, task_type: str = "inference_v1") -> RoutingDecision:
        """
        Attempt to route a request through Chicago.

        If the circuit breaker is OPEN, this returns immediately
        with a LocalFallback — zero gRPC calls, zero network overhead.
        """
        self.total_requests += 1

        # Step 1: Check circuit breaker
        if not self.circuit_breaker.should_allow_request():
            self.total_circuit_skips += 1
            logger.info(
                "[ROUTE] %s -> LocalFallback (circuit OPEN, zero overhead)",
                request_id[:8],
            )
            return RoutingDecision(
                route_to_chicago=False,
                fallback_to_local=True,
                fallback_reason=FallbackReason.CIRCUIT_OPEN,
                rtt_ms=0.0,
                request_id=request_id,
                circuit_state=self.circuit_breaker.state,
            )

        # Step 2: Send request with strict 20ms timeout
        request = ember_pb2.RouteRequest(
            request_id=request_id,
            task_type=task_type,
            sent_at_ms=int(time.time() * 1000),
        )

        start = time.perf_counter()

        try:
            response = self.stub.RequestRoute(
                request, timeout=self.timeout_s
            )  # type: ignore[attr-defined]
            rtt_ms = (time.perf_counter() - start) * 1000

            # Success — record and return
            self.total_successes += 1
            self.circuit_breaker.record_success()

            logger.info(
                "[ROUTE] %s -> Chicago (%.1fms) | decision=%s",
                request_id[:8],
                rtt_ms,
                "LOCAL" if response.decision == 0 else "REGIONAL",
            )

            return RoutingDecision(
                route_to_chicago=True,
                fallback_to_local=False,
                fallback_reason=FallbackReason.NONE,
                rtt_ms=rtt_ms,
                request_id=request_id,
                circuit_state=self.circuit_breaker.state,
                response=response,
            )

        except grpc.RpcError as e:
            rtt_ms = (time.perf_counter() - start) * 1000

            # Classify the failure
            if hasattr(e, "code"):
                code = e.code()
                if code == grpc.StatusCode.DEADLINE_EXCEEDED:
                    self.total_timeouts += 1
                    reason = FallbackReason.TIMEOUT
                    logger.warning(
                        "[ROUTE] %s -> TIMEOUT (%.1fms > %dms) | LocalFallback",
                        request_id[:8],
                        rtt_ms,
                        ROUTING_TIMEOUT_MS,
                    )
                elif code == grpc.StatusCode.UNAVAILABLE:
                    self.total_errors += 1
                    reason = FallbackReason.PARTITION
                    logger.warning(
                        "[ROUTE] %s -> PARTITION (%.1fms) | LocalFallback",
                        request_id[:8],
                        rtt_ms,
                    )
                else:
                    self.total_errors += 1
                    reason = FallbackReason.ERROR
                    logger.warning(
                        "[ROUTE] %s -> ERROR %s (%.1fms) | LocalFallback",
                        request_id[:8],
                        code.name,
                        rtt_ms,
                    )
            else:
                self.total_errors += 1
                reason = FallbackReason.ERROR
                logger.warning(
                    "[ROUTE] %s -> ERROR (%.1fms) | LocalFallback",
                    request_id[:8],
                    rtt_ms,
                )

            # Record failure — may trip the breaker
            self.circuit_breaker.record_failure()

            return RoutingDecision(
                route_to_chicago=False,
                fallback_to_local=True,
                fallback_reason=reason,
                rtt_ms=rtt_ms,
                request_id=request_id,
                circuit_state=self.circuit_breaker.state,
            )

    def close(self) -> None:
        """Close the gRPC channel and release resources."""
        self.channel.close()

    def status(self) -> dict:
        """Snapshot of routing client state."""
        return {
            "total_requests": self.total_requests,
            "total_successes": self.total_successes,
            "total_timeouts": self.total_timeouts,
            "total_errors": self.total_errors,
            "total_circuit_skips": self.total_circuit_skips,
            **self.circuit_breaker.status(),
        }

    def __enter__(self) -> "ResilientRoutingClient":  # noqa: UP037
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
