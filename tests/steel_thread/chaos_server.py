"""
EMBER Failure Injection Suite — "The Chaos Monkey"

A programmable mock of the Chicago Regional Controller that can simulate:
  - Gray Failures: Latency spikes via EMBER_SIMULATED_DELAY env var
  - Hard Failures: Network partitions via EMBER_SIMULATED_PARTITION env var

This is NOT the production Chicago server. It is a test double that
wraps the real RegionalRouterServicer and injects configurable faults.

Environment Variables:
    EMBER_SIMULATED_DELAY       Delay in milliseconds (e.g., "50")
    EMBER_SIMULATED_PARTITION   "true" to simulate unreachability

These can also be set programmatically via the class interface for
test automation without touching env vars.
"""

import os
import sys
import time
import logging
from concurrent import futures

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import grpc

import ember_pb2
import ember_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ember.chaos")

SERVER_PORT = 50051


class ChaosRouterServicer(ember_pb2_grpc.RegionalRouterServicer):
    """
    Programmable mock of the Chicago Regional Controller.

    Supports two failure modes:
      1. Latency injection: adds a configurable delay before responding
      2. Partition simulation: drops the request entirely (gRPC UNAVAILABLE)

    Failure modes can be toggled at runtime — no server restart required.
    """

    def __init__(self) -> None:
        self.requests_handled = 0
        self._simulated_delay_ms: float = 0.0
        self._simulated_partition: bool = False

        # Read initial state from environment
        env_delay = os.environ.get("EMBER_SIMULATED_DELAY", "0")
        try:
            self._simulated_delay_ms = float(env_delay)
        except ValueError:
            self._simulated_delay_ms = 0.0

        env_partition = os.environ.get("EMBER_SIMULATED_PARTITION", "false")
        self._simulated_partition = env_partition.lower() == "true"

    @property
    def simulated_delay_ms(self) -> float:
        return self._simulated_delay_ms

    @simulated_delay_ms.setter
    def simulated_delay_ms(self, value: float) -> None:
        old = self._simulated_delay_ms
        self._simulated_delay_ms = value
        if old != value:
            logger.info("[CHAOS] Delay changed: %.1fms -> %.1fms", old, value)

    @property
    def simulated_partition(self) -> bool:
        return self._simulated_partition

    @simulated_partition.setter
    def simulated_partition(self, value: bool) -> None:
        old = self._simulated_partition
        self._simulated_partition = value
        if old != value:
            state = "ACTIVE" if value else "CLEARED"
            logger.info("[CHAOS] Partition %s", state)

    def RequestRoute(
        self,
        request: ember_pb2.RouteRequest,
        context: grpc.ServicerContext,
    ) -> ember_pb2.RouteResponse:
        self.requests_handled += 1

        # Hard failure: simulate network partition
        if self._simulated_partition:
            logger.warning(
                "[CHAOS] PARTITION | request_id=%s -> UNAVAILABLE",
                request.request_id[:8],
            )
            context.abort(grpc.StatusCode.UNAVAILABLE, "simulated partition")
            # context.abort raises, but return for type safety
            return ember_pb2.RouteResponse()

        # Gray failure: inject latency
        if self._simulated_delay_ms > 0:
            delay_s = self._simulated_delay_ms / 1000.0
            logger.info(
                "[CHAOS] DELAY %.1fms | request_id=%s",
                self._simulated_delay_ms,
                request.request_id[:8],
            )
            time.sleep(delay_s)

        # Normal response
        response = ember_pb2.RouteResponse(
            request_id=request.request_id,
            decision=ember_pb2.EXECUTE_LOCAL,
            responded_at_ms=int(time.time() * 1000),
            reason="chaos: routing mock",
        )

        return response

    def clear_faults(self) -> None:
        """Remove all injected faults."""
        self.simulated_delay_ms = 0.0
        self.simulated_partition = False
        logger.info("[CHAOS] All faults cleared")


def serve(port: int = SERVER_PORT) -> tuple[grpc.Server, ChaosRouterServicer]:
    """
    Start the Chaos Chicago server.

    Returns the server and servicer so tests can control faults
    programmatically without environment variables.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    servicer = ChaosRouterServicer()
    ember_pb2_grpc.add_RegionalRouterServicer_to_server(servicer, server)

    listen_address = f"[::]:{port}"
    server.add_insecure_port(listen_address)
    server.start()

    logger.info("[CHAOS] Chicago mock listening on %s", listen_address)
    logger.info(
        "[CHAOS] Initial state: delay=%.1fms, partition=%s",
        servicer.simulated_delay_ms,
        servicer.simulated_partition,
    )

    return server, servicer


if __name__ == "__main__":
    server, servicer = serve()
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(
            "[CHAOS] Shutting down. Total requests: %d", servicer.requests_handled
        )
        server.stop(grace=5)