import logging
from multiprocessing import Process, Queue
import json

from mlflow import MlflowClient
from middleware.consumer import Consumer
from server.batch_handler import BatchHandler
from server.shutdown_monitor import ShutdownMonitor
import pika


class ClientManager(Process):
    def __init__(
        self,
        message_data: tuple,
        middleware_config,
        shutdown_queue: Queue,
        remove_handler_callback=None,
        add_client_callback=None,
    ):
        """
        Initialize ClientManager as a Process.

        Args:
            message_data: Tuple containing (properties, body) from RabbitMQ (NO ch, NO method)
            middleware_config: Middleware config object
            shutdown_queue: Queue to receive shutdown signal
            remove_handler_callback: Function to call when client finishes (from Listener)
        """
        super().__init__()
        self.message_data = message_data
        self.middleware_config = middleware_config
        self.shutdown_queue = shutdown_queue
        self.consumer = None
        self.batch_handler = None
        self.shutdown_monitor = None
        self.shutdown_initiated = False
        self.client_id = None
        self.logger = logging.getLogger("client-manager")
        self._remove_handler_callback = remove_handler_callback
        self._add_client = add_client_callback

    def _handle_shutdown_signal(self):
        """Callback function called by ShutdownMonitor when shutdown signal is received."""
        self.logger.info(f"Shutdown callback invoked for client {self.client_id}")
        self.shutdown_initiated = True
        self._wait_for_consumer()
        self.batch_handler.stop_processing()

    def _wait_for_consumer(self):
        """Wait for consumer to finish."""
        try:
            if self.consumer:
                self.consumer.shutdown()  # Signal consumer to shutdown
                self.logger.info(f"Consumer joined for client {self.client_id}")
        except Exception as e:
            self.logger.error(
                f"Error waiting for handlers for client {self.client_id}: {e}"
            )

    def run(self):
        """
        Main process loop: parse message, setup queues, create consumer, and start processing.
        """
        # Start shutdown monitor
        self.shutdown_monitor = ShutdownMonitor(
            shutdown_queue=self.shutdown_queue,
            shutdown_callback=self._handle_shutdown_signal,
        )
        self.shutdown_monitor.start()

        if not self._process_notification_message():
            return

        if self._add_client:
            self._add_client(self.client_id, self)

        try:
            if self.shutdown_initiated:
                # If shutdown was initiated while processing the message, exit early
                return
            self._setup_consumer()

            self.logger.info("ANASHE")
            # If shutdown was received is not necessary to remove the handler
            if self._remove_handler_callback and not self.shutdown_initiated:
                self._remove_handler_callback(self.client_id)

        except Exception as e:
            self.logger.error(f"Error setting up client {self.client_id}: {e}")

    def _process_notification_message(self):
        """Parse and validate the notification message. Returns True if valid, False otherwise."""
        self.properties, self.body = self.message_data
        try:
            notification = json.loads(self.body.decode("utf-8"))
        except Exception as e:
            self.logger.error(
                f"Failed to parse client notification: {e} | body: {self.body}"
            )
            return False

        self.client_id = notification.get("client_id")
        if not self.client_id:
            self.logger.error(f"Client notification missing client_id: {notification}")
            return False

        # Update logger with client_id
        self.logger = logging.getLogger(f"client-manager-{self.client_id}")

        self.logger.info(
            f"Processing new client notification | client_id: {self.client_id} | "
            f"inputs_format: {notification.get('inputs_format','')} | "
            f"outputs_format: {notification.get('outputs_format','')} | "
            f"model_type: {notification.get('model_type','')}"
        )
        return True

    def _setup_consumer(self):
        """Setup BatchHandler and Consumer thread for this client."""
        self.logger.info(f"ClientManager process started for client {self.client_id}")

        # Setup BatchHandler
        self.batch_handler = BatchHandler(
            client_id=self.client_id,
            mlflow_client=MlflowClient(),
            on_eof=self._handle_EOF_message,
        )

        self.consumer = Consumer(
            middleware_config=self.middleware_config,
            client_id=self.client_id,
            labeled_callback=self._handle_labeled_message,
            replies_callback=self._handle_replies_message,
            logger=self.logger,
        )
        self.consumer.start()
        self.consumer.join()  # Wait for consumer to finish

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
        self.consumer.shutdown()
