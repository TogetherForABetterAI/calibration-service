import logging
import signal
from middleware.middleware import Middleware
from server.listener import Listener
from lib.config import CONNECTION_QUEUE_NAME, DATASET_EXCHANGE


class Server():
    """
    Server handles RabbitMQ server operations for client notifications.
    """

    def __init__(self, config, middleware_cls, cm_middleware_factory, mlflow_logger_factory):
        self.config = config
        self.logger = logging.getLogger("calibration-server")
        self.logger.info("Initializing Calibration Server")

        # Establish RabbitMQ connection and middleware
        self.logger.info("Connecting to RabbitMQ...")
        self.middleware = middleware_cls
        channel = self.middleware.create_channel(prefetch_count=1)
        self.middleware.setup_connection_queue(channel, durable=False)

        self.logger.info("Initializing Listener...")
        self.listener = Listener(middleware=self.middleware, channel=channel, cm_middleware_factory=cm_middleware_factory, mlflow_logger_factory=mlflow_logger_factory)

        self.logger.info(
            f"Server initialized - ready to consume from {self.config.middleware_config.host}"
        )
        self._shutdown_received = False
        signal.signal(signal.SIGINT, lambda s, f: self.stop())
        signal.signal(signal.SIGTERM, lambda s, f: self.stop())

    def run(self):
        """
        Start the Listener class to begin consuming client notifications.
        """
        self.logger.info("Starting listener for client notifications...")
        if self._shutdown_received:
            self.logger.info("Shutdown already received, not starting listener")
            return
        try:
            self.listener.start()
        except Exception as e:
            self.logger.error(f"Failed to start listener: {e}")
            raise e
        finally:
            self.stop()

    def stop(self):
        """
        Gracefully stop the server by signaling the listener to shutdown.
        """
        self.logger.info("Initiating graceful server shutdown")
        try:
            self._shutdown_received = True
            self.listener.stop_consuming()
            self.logger.info("Server shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during server shutdown: {e}")
