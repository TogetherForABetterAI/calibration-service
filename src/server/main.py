import logging
import threading
from threading import Thread
from middleware.middleware import Middleware
from server.listener import Listener
from lib.config import DATASET_EXCHANGE


class Server(Thread):
    """
    Server handles RabbitMQ server operations for client notifications.
    """

    def __init__(self, config):
        super().__init__()  # Initialize Thread base class
        self.config = config
        self.logger = logging.getLogger("calibration-server")
        self.logger.info("Initializing Calibration Server")

        # Establish RabbitMQ connection and middleware
        self.logger.info("Connecting to RabbitMQ...")
        self.middleware = Middleware(self.config.middleware_config)
        channel = self.middleware.create_channel(prefetch_count=1)
        self.middleware.setup_connection_queue(channel, durable=False)
        # Initialize listener
        self.logger.info("Initializing Listener...")
        self.listener = Listener(
            middleware=self.middleware,
            channel=channel,
            upper_bound_clients=self.config.server_config.upper_bound_clients,
            lower_bound_clients=self.config.server_config.lower_bound_clients,
            replica_id=self.config.server_config.replica_id,
            replica_timeout_seconds=self.config.server_config.replica_timeout_seconds,
            logger=self.logger
        )

    def run(self):
        """
        Start the Listener class to begin consuming client notifications.
        """
        self.logger.info("Starting listener for client notifications...")
        try:
            self.listener.start()
        except Exception as e:
            self.logger.error(f"Failed to start listener: {e}")
            raise e

    def stop(self):
        """
        Gracefully stop the server by signaling the listener to shutdown.
        """
        self.logger.info("Initiating graceful server shutdown")
        try:
            self.listener.stop_consuming()
            self.logger.info("Server shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during server shutdown: {e}")
