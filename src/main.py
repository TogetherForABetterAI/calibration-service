import signal
import sys
import logging
from core.client_manager import ClientManager
from gRPC.server import GrpcServer
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logging.info("Received shutdown signal, cleaning up...")
    if hasattr(signal_handler, "client_manager"):
        signal_handler.client_manager.shutdown_all()
    if hasattr(signal_handler, "grpc_server"):
        signal_handler.grpc_server.stop()
    sys.exit(0)


def main():
    """Main entry point for the calibration service."""
    try:
        # Initialize client manager
        client_manager = ClientManager()
        signal_handler.client_manager = client_manager

        # Initialize and start gRPC server
        grpc_server = GrpcServer(client_manager, port=settings.GRPC_PORT)
        signal_handler.grpc_server = grpc_server

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start the gRPC server
        grpc_server.start()

        logging.info("Calibration service started successfully")
        logging.info(f"gRPC server listening on port {settings.GRPC_PORT}")
        logging.info("Ready to accept client registrations")

        # Wait for the server to terminate
        grpc_server.wait_for_termination()

    except Exception as e:
        logging.error(f"Failed to start calibration service: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
