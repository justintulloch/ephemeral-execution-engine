import os
import sys
import time
import logging
from concurrent import futures

# Add project root to path so we can find ember_pb2 and core/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
 
import grpc

import ember_pb2
import ember_pb2_grpc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# gRPC server port — standard gRPC convention
SERVER_PORT = 50051


class RegionalRouterServicer(ember_pb2_grpc.RegionalRouterServicer):
    """
    Skeleton implementation of the RegionalRouter service.

    """

    def __init__(self) -> None:
        self.requests_handled = 0

    def RequestRoute(
        self,
        request: ember_pb2.RouteRequest,
        context: grpc.ServicerContext,
    ) -> ember_pb2.RouteResponse:
        self.requests_handled += 1
        response = ember_pb2.RouteResponse(
            request_id=request.request_id,
            decision=ember_pb2.EXECUTE_LOCAL,
            responded_at_ms=int(time.time() * 1000),
            reason="skeleton: no routing logic active",
        )

        if self.requests_handled % 200 == 0:
            logger.info(
                "Handled %d requests | latest request_id=%s",
                self.requests_handled,
                request.request_id,
            )

        return response

def serve() -> None:
    """Start the Chicago Regional Controller gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    servicer = RegionalRouterServicer()
    ember_pb2_grpc.add_RegionalRouterServicer_to_server(servicer, server)

    listen_address = f"[::]:{SERVER_PORT}"
    server.add_insecure_port(listen_address)
    server.start()

    logger.info("Chicago Regional Controller listening on %s", listen_address)
    logger.info("Skeleton mode — all requests return EXECUTE_LOCAL")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(
            "Shutting down. Total requests handled: %d",
            servicer.requests_handled,
        )
        server.stop(grace=5)

if __name__ == "__main__":
    serve()