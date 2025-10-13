import logging
import threading
from threading import Thread
from middleware.middleware import Middleware
from server.listener import Listener
from lib.constants import DATASET_EXCHANGE


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

        # Initialize listener
        self.logger.info("Initializing Listener...")
        self.listener = Listener(middleware=self.middleware)

        self.logger.info(
            f"Server initialized - ready to consume from {self.config.middleware_config.host}"
        )

    def run(self):
        """
        Start consuming client notification messages from the existing queue.
        This method runs in a separate thread when start() is called.
        """
        self.logger.info("Starting listener for client notifications...")
        try:
            self.listener.start()
            self.logger.info("Listener started successfully")
            self.listener.join()
        except Exception as e:
            self.logger.error(f"Failed to start listener: {e}")
            raise e

    def stop(self):
        """
        Gracefully stop the server by signaling the listener to shutdown.
        """
        self.logger.info("Initiating graceful server shutdown")
        try:
            # Signal shutdown to the listener via its queue
            if self.listener:
                self.logger.info("Sending shutdown signal to listener...")
                self.listener.shutdown_queue.put(None)

                # Wait for listener thread to finish
                self.logger.info("Waiting for listener to finish...")
                self.listener.join()

            self.logger.info("Server shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during server shutdown: {e}")
