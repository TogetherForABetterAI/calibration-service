import logging
import signal
from middleware.middleware import Middleware
from server.listener import Listener
from lib.config import CONNECTION_QUEUE_NAME


class Server():
    """
    Server handles RabbitMQ server operations for client notifications.
    """

    def __init__(self, config, middleware_cls, cm_middleware_factory, report_builder_factory, database):
        self.config = config
        self.logger = logging.getLogger("calibration-server")
        self.logger.info("Initializing Calibration Server")

        # Establish RabbitMQ connection and middleware
        self.logger.info("Connecting to RabbitMQ...")
        self.middleware = middleware_cls
        channel = self.middleware.create_channel(prefetch_count=self.config.server_config.upper_bound_clients)
        self.middleware.setup_connection_queue(channel, durable=True)

        self.logger.info("Initializing Listener...")
        self.listener = Listener(
            middleware=self.middleware,
            channel=channel,
            config=self.config.server_config,
            cm_middleware_factory=cm_middleware_factory,
            report_builder_factory=report_builder_factory,
            logger=self.logger,
            database=database
        )

        self.logger.info(
            f"Server initialized - ready to consume from {self.config.middleware_config.host}"
        )
        self._shutdown_received = False
        signal.signal(signal.SIGINT, lambda s, f: self.handle_sigterm())
        signal.signal(signal.SIGTERM, lambda s, f: self.handle_sigterm())

    def run(self):
        """
        Start the Listener class to begin consuming client notifications.
        """
        self.logger.info("Starting listener for client notifications...")
        if self._shutdown_received:
            self.logger.info("Shutdown already received, not starting listener")
            return
        self.listener.start()
       
    def handle_sigterm(self):
        """
        Gracefully handle_sigterm the server by signaling the listener to shutdown.
        """
        self.logger.info("Initiating graceful server shutdown")
        try:
            self._shutdown_received = True
            self.listener.handle_sigterm()
            self.logger.info("Server shutdown completed")
        except Exception as e:
            self.logger.error(f"Error during server shutdown: {e}")
