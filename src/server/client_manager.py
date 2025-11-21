import logging
from multiprocessing import Process, Queue
import signal
from middleware.consumer import Consumer
from server.batch_handler import BatchHandler
from src.lib.config import MLFLOW_EXCHANGE
from src.middleware.middleware import Middleware


class ClientManager(Process):
    def __init__(
        self,
        client_id: str,
        middleware,
        clients_to_remove_queue: Queue,
        report_builder,
    ):
        """
        Initialize ClientManager as a Process.

        Args:
            client_id: The client ID (parsed by Listener before process creation)
            middleware_config: Middleware config object (NOT the middleware instance itself)
            clients_to_remove_queue: Queue to send removal requests to parent process
        """
        super().__init__()
        self.logger = logging.getLogger(f"client-manager-{client_id}")
        self.logger.info(f"Initializing ClientManager for client {client_id}")
        self.client_id = client_id
        self.middleware = middleware
        self.clients_to_remove_queue = clients_to_remove_queue
        self.consumer = None
        self.batch_handler = None
        self.shutdown_initiated = False
        self.report_builder = report_builder
        logging.info(f"ClientManager for client {client_id} initialized")


    def _handle_shutdown_signal(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown."""
        self.logger.info(f"Received SIGTERM signal for client {self.client_id}")
        self.shutdown_initiated = True
        self.consumer.handle_sigterm()
        self.batch_handler.handle_sigterm()

    def run(self):
        """
        Main process loop: parse message, setup queues, create consumer, and start processing.
        Each process creates its own RabbitMQ connection to avoid conflicts.
        """
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

        try:
            logging.info(f"ClientManager process started for client {self.client_id}")
            self.batch_handler = BatchHandler(
                client_id=self.client_id,
                on_eof=self._handle_EOF_message,    
                report_builder=self.report_builder,
                middleware=self.middleware,
            )

            self.consumer = Consumer(
                middleware=self.middleware,
                client_id=self.client_id,
                predictions_callback=self._handle_predictions_message,
                inputs_callback=self._handle_inputs_message,
                logger=self.logger,
            )

            if not self.shutdown_initiated:
                self.consumer.start()  
        
            if not self.shutdown_initiated and self.clients_to_remove_queue:
                self.clients_to_remove_queue.put(self.client_id)
                
        except Exception as e:
            self.logger.error(f"Error setting up client {self.client_id}: {e}")

    def _handle_predictions_message(self, ch, method, properties, body):
        """Callback for replies queue - calls BatchHandler._handle_predictions_message"""
        self.logger.info(f"Received predictions message for client {self.client_id}")
        self.batch_handler._handle_predictions_message(ch, body)

    def _handle_inputs_message(self, ch, method, properties, body):
        """Callback for inputs queue - calls BatchHandler._handle_inputs_message"""
        self.logger.info(f"Received inputs message for client {self.client_id}")
        self.batch_handler._handle_inputs_message(ch, body)


    def _handle_EOF_message(self):
        """Handle end-of-file message: stop consumer and batch handler, then remove client from active_clients."""
        self.logger.info(f"Received EOF message for client {self.client_id}")
        self.batch_handler.handle_sigterm()
        self.consumer.handle_sigterm()
