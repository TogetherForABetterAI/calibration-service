import logging
import threading
from threading import Thread
from middleware.middleware import Middleware
from server.listener import Listener
from lib.config import CONNECTION_QUEUE_NAME, DATASET_EXCHANGE


class Server(Thread):
    """
    Server handles RabbitMQ server operations for client notifications.
    """

    def __init__(self, config, middleware_cls):
        threading.Thread.__init__(self)
        self.config = config
        self.logger = logging.getLogger("calibration-server")
        self.logger.info("Initializing Calibration Server")

        # Establish RabbitMQ connection and middleware
        self.logger.info("Connecting to RabbitMQ...")
        self.middleware = middleware_cls
        channel = self.middleware.create_channel(prefetch_count=1)
        self.middleware.setup_connection_queue(channel, durable=False)

        self.logger.info("Initializing Listener...")
        self.listener = Listener(middleware=self.middleware, channel=channel)

        self.logger.info(
            f"Server initialized - ready to consume from {self.config.middleware_config.host}"
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
            print("Server run method called")
            self.listener.stop_consuming()
            self.logger.info("Server shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during server shutdown: {e}")
