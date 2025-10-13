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
        client_id: str,
        message_data: tuple,
        middleware_config,
        shutdown_queue: Queue,
        remove_client_queue: Queue = None,
    ):
        """
        Initialize ClientManager as a Process.

        Args:
            client_id: The client ID (parsed by Listener before process creation)
            message_data: Tuple containing (properties, body) from RabbitMQ (NO ch, NO method)
            middleware_config: Middleware config object
            shutdown_queue: Queue to receive shutdown signal
            remove_client_queue: Queue to send removal requests to parent process
        """
        super().__init__()
        self.client_id = client_id
        self.message_data = message_data
        self.middleware_config = middleware_config
        self.shutdown_queue = shutdown_queue
        self.remove_client_queue = remove_client_queue
        self.consumer = None
        self.batch_handler = None
        self.shutdown_monitor = None
        self.shutdown_initiated = False
        self.logger = logging.getLogger(f"client-manager-{client_id}")

    def _handle_shutdown_signal(self):
        """Callback function called by ShutdownMonitor when shutdown signal is received."""
        self.logger.info(f"Shutdown callback invoked for client {self.client_id}")
        self.shutdown_initiated = True
        self.consumer.stop_consuming()
        self.batch_handler.stop_processing()

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

        try:
            if self.shutdown_initiated:
                return  # If shutdown was initiated while processing the message, exit early

            self._start_consumer()
            self.consumer.join()  # Ensure consumer is finished

            # Notify parent that this client has finished
            if self.remove_client_queue and not self.shutdown_initiated:
                self.remove_client_queue.put(self.client_id)
        except Exception as e:
            self.logger.error(f"Error setting up client {self.client_id}: {e}")

    def _start_consumer(self):
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
