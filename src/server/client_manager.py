import logging
from multiprocessing import Process, Queue
import signal
from mlflow import MlflowClient
from middleware.consumer import Consumer
from server.batch_handler import BatchHandler
from src.middleware.middleware import Middleware


class ClientManager(Process):
    def __init__(
        self,
        client_id: str,
        middleware_config,
        remove_client_queue: Queue,
    ):
        """
        Initialize ClientManager as a Process.

        Args:
            client_id: The client ID (parsed by Listener before process creation)
            middleware_config: Middleware config object (NOT the middleware instance itself)
            remove_client_queue: Queue to send removal requests to parent process
        """
        super().__init__()
        self.client_id = client_id
        self.middleware_config = middleware_config
        self.middleware = Middleware(middleware_config)  # create new connection
        self.remove_client_queue = remove_client_queue
        self.consumer = None
        self.batch_handler = None
        self.shutdown_initiated = False
        self.logger = logging.getLogger(f"client-manager-{client_id}")

        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)


    def _handle_shutdown_signal(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown."""
        self.logger.info(f"Received SIGTERM signal for client {self.client_id}")
        self.shutdown_initiated = True
        self.consumer.set_shutdown()
        self.batch_handler.stop_processing()

    def run(self):
        """
        Main process loop: parse message, setup queues, create consumer, and start processing.
        Each process creates its own RabbitMQ connection to avoid conflicts.
        """
        try:
            self.batch_handler = BatchHandler(
                client_id=self.client_id,
                mlflow_client=MlflowClient(),
                on_eof=self._handle_EOF_message,
            )

            self.consumer = Consumer(
                middleware=self.middleware,
                client_id=self.client_id,
                labeled_callback=self._handle_labeled_message,
                replies_callback=self._handle_replies_message,
                logger=self.logger,
            )

            if not self.shutdown_initiated:
                self.consumer.start()  
        
            if not self.shutdown_initiated:
                self.remove_client_queue.put(self.client_id)
                
        except Exception as e:
            self.logger.error(f"Error setting up client {self.client_id}: {e}")

    # Define callbacks that call BatchHandler methods
    def _handle_labeled_message(self, ch, method, properties, body):
        """Callback for labeled queue - calls BatchHandler._handle_data_message"""
        self.logger.info(f"Received labeled message for client {self.client_id}")
        self.batch_handler._handle_data_message(body)

    def _handle_replies_message(self, ch, method, properties, body):
        """Callback for replies queue - calls BatchHandler._handle_probability_message"""
        self.logger.info(f"Received replies message for client {self.client_id}")
        self.batch_handler._handle_probability_message(body)

    def _handle_EOF_message(self):
        """Handle end-of-file message: stop consumer and batch handler, then remove client from active_clients."""
        self.logger.info(f"Received EOF message for client {self.client_id}")
        self.batch_handler.stop_processing()
        self.consumer.stop_consuming()
