import os
import signal
import sys
import logging
import threading
from server.main import Server
from lib.logger import initialize_logging
from lib.config import initialize_config
from src.middleware.middleware import Middleware


def setup_logging(config):
    level = config.log_level.upper()
    initialize_logging(level)


def signal_handler(quit_event, signum, frame):
    logging.info("Received shutdown signal, cleaning up...")
    quit_event.set()


def start_service_with_graceful_shutdown(server, config):
    quit_event = threading.Event()

    # Register signal handlers
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(quit_event, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(quit_event, s, f))

    # Start server in its own thread
    logging.info("Starting service: %s", config.server_config.service_name)
    server.start()

    logging.info("Server started successfully")

    # Wait for shutdown signal
    quit_event.wait()
    logging.info("Shutting down service...")

    server.stop()

    # Wait for server thread to finish
    logging.info("Waiting for server thread to finish...")
    server.join()
    logging.info("Service exited gracefully")


def main():
    config = initialize_config()
    setup_logging(config)
    middleware = Middleware(config.middleware_config)
    server = Server(config, middleware_cls=middleware)
    logging.info("Initialized server, starting main service loop")
    start_service_with_graceful_shutdown(server, config)
    


if __name__ == "__main__":
    main()
