import time
import uuid
import logging

import grpc

import ember_pb2
import ember_pb2_grpc
from core.constants import ROUTING_TIMEOUT_MS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_TARGET = "localhost:50051"


class RoutingClient:
    """
    Client that sends RouteRequests to the Chicago Regional Controller.

    Enforces the 20ms timeout defined in the system design.
    If Chicago doesn't respond in time, the caller should fall back
    to local execution.
    """

    def __init__(self, target: str = DEFAULT_TARGET) -> None:
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = ember_pb2_grpc.RegionalRouterStub(self.channel)
        self.timeout_s = ROUTING_TIMEOUT_MS / 1000.0

    def request_route(self, task_type: str = "ping") -> tuple[ember_pb2.RouteResponse | None, float]:
        """
        Send a single RouteRequest to Chicago.

        Returns:
            (response, rtt_ms) — the response and measured round-trip time.
            If the request times out, response is None.
        """
        request_id = str(uuid.uuid4())
        sent_at_ms = int(time.time() * 1000)

        request = ember_pb2.RouteRequest(
            request_id=request_id,
            task_type=task_type,
            sent_at_ms=sent_at_ms,
        )

        start = time.perf_counter()

        try:
            response = self.stub.RequestRoute(request, timeout=self.timeout_s) # type: ignore[attr-defined]
            rtt_ms = (time.perf_counter() - start) * 1000
            return response, rtt_ms
        except grpc.RpcError as e:
            rtt_ms = (time.perf_counter() - start) * 1000
            logger.warning(
                "RouteRequest failed | request_id=%s | rtt=%.2fms | error=%s",
                request_id,
                rtt_ms,
                e.code().name if hasattr(e, "code") else str(e),
            )
            return None, rtt_ms

    def close(self) -> None:
        """Close the gRPC channel."""
        self.channel.close()

    def __enter__(self) -> RoutingClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()