from http import HTTPStatus
import logging
from multiprocessing import Process, Queue
import os
import signal
import threading
from time import time, sleep
from middleware.consumer import Consumer
from server.batch_handler import BatchHandler
import requests
from enum import Enum

from src.lib.session_status import SessionStatus


class ClientManager(Process):
    def __init__(
        self,
        client_id: str,
        session_id: str,
        middleware,
        clients_to_remove_queue: Queue,
        config,
        report_builder,
        database=None,
        inputs_format=None,
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
        self.database = database
        self.session_id = session_id
        self.inputs_format = inputs_format

        # Timeout management
        self.connections_service_url = os.getenv("CONNECTIONS_SERVICE_URL", "http://connections-service:8000")
        self.client_timeout_seconds = config.client_timeout_seconds
        self.last_message_time = time()
        self.last_message_time_lock = threading.Lock()
        self.timeout_checker_handler = threading.Thread(target=self._timeout_checker)
        self.timeout_checker_handler.daemon = True
        self.status = SessionStatus.IN_PROGRESS
        
        logging.info(f"ClientManager for client {client_id} initialized")

    def _timeout_checker(self):
        """Periodically check for timeouts in BatchHandler."""
        check_interval = self.client_timeout_seconds / 2
    
        while not self.shutdown_initiated:
            with self.last_message_time_lock:
                if (time() - self.last_message_time > self.client_timeout_seconds):
                    logging.info(f"Client {self.client_id} timed out due to inactivity.")
                    self.update_session_status(SessionStatus.TIMEOUT)
                    return

            for _ in range(int(check_interval * 10)):  
                if self.shutdown_initiated:
                    return
                sleep(0.1)


    def _handle_shutdown_signal(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown."""
        self.logger.info(f"Received SIGTERM signal for client {self.client_id}")
        self.shutdown_initiated = True
        if self.consumer:
            self.consumer.handle_sigterm()
        if self.batch_handler:
            self.batch_handler.handle_sigterm()
        if self.timeout_checker_handler.is_alive():
            self.timeout_checker_handler.join(timeout=2)


    def run(self):
        """
        Main process loop: parse message, setup queues, create consumer, and start processing.
        Each process creates its own RabbitMQ connection to avoid conflicts.
        """
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        self.timeout_checker_handler.start()

        try:
            logging.info(f"ClientManager process started for client {self.client_id}")
            self.batch_handler = BatchHandler(
                client_id=self.client_id,
                session_id=self.session_id,
                on_eof=self._handle_EOF_message,    
                report_builder=self.report_builder,
                middleware=self.middleware,
                database=self.database,
                inputs_format=self.inputs_format,
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
        with self.last_message_time_lock:
            self.last_message_time = time()
        self.batch_handler._handle_predictions_message(ch, body)

    def _handle_inputs_message(self, ch, method, properties, body):
        """Callback for inputs queue - calls BatchHandler._handle_inputs_message"""
        self.logger.info(f"Received inputs message for client {self.client_id}")
        with self.last_message_time_lock:
            self.last_message_time = time()
        self.batch_handler._handle_inputs_message(ch, body)

    def update_session_status(self, session_status):
        """Update the session status to the given status in the connections service."""
        try:
            logging.info(f"Updating session {self.session_id} status to {session_status.name()}")
            url = f"{self.connections_service_url}/sessions/{self.session_id}/status"
            headers = {"Content-Type": "application/json"}
            response = requests.put(url, json={"status": session_status.name()}, headers=headers)
            self.status = session_status

            if response.status_code == HTTPStatus.OK:
                self.logger.info(f"Session {self.session_id} status updated to {session_status.name()}.")
            else:
                self.logger.error(f"Failed to update session {self.session_id} status. Response code: {response.status_code}, Response body: {response.text}. Status: {session_status.name()}")
        except Exception as e:
            self.logger.error(f"Error updating session {self.session_id} status: {e}")

    def _handle_EOF_message(self):
        """Handle end-of-file message: stop consumer and batch handler, then remove client from active_clients."""
        self.logger.info(f"Received EOF message for client {self.client_id}")
        self.consumer.handle_sigterm()
        self.batch_handler.handle_sigterm()
        self.update_session_status(SessionStatus.COMPLETED)
        
