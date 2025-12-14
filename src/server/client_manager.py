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
import pika.exceptions

from src.database.db import Database
from src.lib.db_engine import get_engine
from src.lib.session_status import SessionStatus


class ClientManager(Process):
    def __init__(
        self,
        user_id: str,
        session_id: str,
        middleware,
        clients_to_remove_queue: Queue,
        config,
        report_builder,
        utrace_calculator_factory,
        inputs_format=None,
        recipient_email=None,
    ):
        """
        Initialize ClientManager as a Process.

        Args:
            user_id: The client ID (parsed by Listener before process creation)
            middleware_config: Middleware config object (NOT the middleware instance itself)
            clients_to_remove_queue: Queue to send removal requests to parent process
        """
        super().__init__()
        self.logger = logging.getLogger(f"client-manager-{user_id}")
        self.logger.info(f"Initializing ClientManager for client {user_id}")
        self.user_id = user_id
        self.middleware = middleware
        self.clients_to_remove_queue = clients_to_remove_queue
        self.consumer = None
        self.batch_handler = None
        self.shutdown_initiated = False
        self.report_builder = report_builder
        self.database = None
        self.session_id = session_id
        self.inputs_format = inputs_format
        self.recipient_email = recipient_email
        self.utrace_calculator_factory = utrace_calculator_factory
        self.utrace_calculator = None
        self.config = config

        # Timeout management
        self.connections_service_url = os.getenv("CONNECTIONS_SERVICE_URL", "http://connections-service:8000")
        self.last_message_time = time()
        self.last_message_time_lock = threading.Lock()
        self.timeout_checker_handler = threading.Thread(target=self._timeout_checker)
        self.timeout_checker_handler.daemon = True

        self.status_lock = threading.Lock()
        self.status = SessionStatus.IN_PROGRESS
        
        logging.info(f"ClientManager for client {user_id} initialized")

    def _timeout_checker(self):
        """Periodically check for timeouts in BatchHandler."""
        check_interval = self.config.server_config.client_timeout_seconds / 2
    
        while not self.shutdown_initiated:
            with self.last_message_time_lock:
                if (time() - self.last_message_time > self.config.server_config.client_timeout_seconds):
                    logging.info(f"Client {self.user_id} timed out due to inactivity.")
                    self.update_session_status(SessionStatus.TIMEOUT)
                    self._initiate_shutdown(source_thread=threading.current_thread())
                    return

            for _ in range(int(check_interval * 10)):  
                if self.shutdown_initiated:
                    return
                sleep(0.1)


    def _handle_shutdown_signal(self, signum, frame):
        """Handle SIGTERM signal for graceful shutdown (or process end)."""
        self.logger.info(f"Received shutdown signal for client {self.user_id}")
        self._initiate_shutdown(source_thread=threading.current_thread())

    def _initiate_shutdown(self, source_thread=None):
        if self.shutdown_initiated:
            return
        
        self.shutdown_initiated = True
        
        if self.consumer:
            self.consumer.handle_sigterm()
        if self.batch_handler:
            self.batch_handler.handle_sigterm()
            
        if (self.timeout_checker_handler.is_alive() and 
            self.timeout_checker_handler is not source_thread): 
            
            self.timeout_checker_handler.join(timeout=2)

    def run(self):
        """
        Main process loop: parse message, setup queues, create consumer, and start processing.
        Each process creates its own RabbitMQ connection to avoid conflicts.
        """
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        self.timeout_checker_handler.start()
        self.database = Database(get_engine(self.config.database_url))
        self.utrace_calculator = self.utrace_calculator_factory(database=self.database, session_id=self.session_id)

        try:
            logging.info(f"ClientManager process started for client {self.user_id}")
            self.batch_handler = BatchHandler(
                user_id=self.user_id,
                session_id=self.session_id,
                on_eof=self._handle_EOF_message,    
                middleware=self.middleware,
                database=self.database,
                inputs_format=self.inputs_format,
                utrace_calculator=self.utrace_calculator,
            )

            self.consumer = Consumer(
                middleware=self.middleware,
                user_id=self.user_id,
                predictions_callback=self._handle_predictions_message,
                inputs_callback=self._handle_inputs_message,
                logger=self.logger,
            )
            self.batch_handler._build_state()

            if not self.shutdown_initiated:
                self.consumer.start()  
        
            if not self.shutdown_initiated and self.clients_to_remove_queue:
                self.clients_to_remove_queue.put(self.user_id)

        except pika.exceptions.AMQPConnectionError as e:
            self.logger.error(f"AMQP Connection error in ClientManager for client {self.user_id}: {e}")   
            # the message will be requeued since the connection was lost before ack
        except Exception as e:
            self.logger.error(f"Error setting up client {self.user_id}: {e}")
        finally:
            self.logger.info(f"ClientManager process for client {self.user_id} terminating")

    def _handle_predictions_message(self, ch, method, properties, body):
        """Callback for replies queue - calls BatchHandler._handle_predictions_message"""
        self.logger.info(f"Received predictions message for client {self.user_id}")
        with self.last_message_time_lock:
            self.last_message_time = time()
        self.batch_handler._handle_predictions_message(ch, body)
        ch.basic_ack(delivery_tag=method.delivery_tag)



    def _handle_inputs_message(self, ch, method, properties, body):
        """Callback for inputs queue - calls BatchHandler._handle_inputs_message"""
        self.logger.info(f"Received inputs message for client {self.user_id}")
        with self.last_message_time_lock:
            self.last_message_time = time()
        self.batch_handler._handle_inputs_message(ch, body)
        ch.basic_ack(delivery_tag=method.delivery_tag)


    def update_session_status(self, session_status):
        """Update the session status to the given status in the connections service."""
        try:
            logging.info(f"Updating session {self.session_id} status to {session_status.name()}")
            status = session_status.name().lower()
            url = f"{self.connections_service_url}/sessions/{self.session_id}/status/{status}"
            headers = {"Content-Type": "application/json"}
            response = requests.put(url, json={"user_id": self.user_id}, headers=headers)
            with self.status_lock:
                self.status = session_status

            if response.status_code == HTTPStatus.OK:
                self.logger.info(f"Session {self.session_id} status updated to {session_status.name()}.")
            else:
                self.logger.error(f"Failed to update session {self.session_id} status. Response code: {response.status_code}, Response body: {response.text}. Status: {session_status.name()}")
        except Exception as e:
            self.logger.error(f"Error updating session {self.session_id} status: {e}")

    def send_report(self):
        """
        Build and send report when both labeled and replies data are complete.
        """
        self.report_builder.generate_report(self.batch_handler.get_calibration_results())  

        logging.info(f"Sending report to {self.recipient_email} for client {self.user_id}")
        self.report_builder.send_report(self.recipient_email)

    def _handle_EOF_message(self):
        """Handle end-of-file message: stop consumer and batch handler, then remove client from active_clients."""

        if self.config.environment == "PRODUCTION":
            self.send_report()

        self.logger.info(f"Received EOF message for client {self.user_id}")
        self.consumer.handle_sigterm()
        self.batch_handler.handle_sigterm()
        self.update_session_status(SessionStatus.COMPLETED)
        
