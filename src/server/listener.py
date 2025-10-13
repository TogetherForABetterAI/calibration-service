import logging
import threading
from threading import Thread
from typing import Dict
from multiprocessing import Queue
from lib.constants import CONNECTION_QUEUE_NAME, CONNECTION_EXCHANGE
from server.client_manager import ClientManager
from server.shutdown_monitor import ShutdownMonitor


class Listener(Thread):
    def __init__(self, middleware, logger=None):
        super().__init__()  # Initialize Thread base class
        self.middleware = middleware
        self.logger = logger or logging.getLogger("listener")
        self.queue_name = CONNECTION_QUEUE_NAME
        self.connection_exchange = CONNECTION_EXCHANGE

        # Queue to receive shutdown signal
        self.shutdown_queue = Queue()

        # Track active client manager processes
        self._active_clients: Dict[str, tuple] = (
            {}
        )  # client_id -> (ClientManager, shutdown_queue)
        self._clients_lock = threading.Lock()

        # Shutdown monitor thread
        self.shutdown_monitor = None
        self.shutdown_initiated = False

        self.logger.info(f"Listener initialized for queue: {self.queue_name}")

    def _handle_shutdown_signal(self):
        """Callback function called by ShutdownMonitor when shutdown signal is received"""
        self.logger.info("Shutdown callback invoked, initiating graceful shutdown...")
        self.shutdown_initiated = True

        self.middleware.shutdown()
        self._shutdown_all_clients()

    def _shutdown_all_clients(self):
        """Shutdown all active client manager processes by enqueuing None in their shutdown_queue."""
        self.logger.info("Shutting down all client managers...")

        # Other threads could try to modify _active_clients
        # so we need to create a copy of the handlers to
        # avoid iteration issues during shutdown and let them
        # finish naturally
        with self._clients_lock:
            clients_to_shutdown = list(self._active_clients.items())

        # Send shutdown signal and wait for each handler
        for client_id, (handler, client_shutdown_queue) in clients_to_shutdown:
            if handler.is_alive():
                try:
                    client_shutdown_queue.put(None)  # Signal shutdown
                    handler.join()
                except Exception as e:
                    self.logger.error(
                        f"Error shutting down ClientManager {client_id}: {e}"
                    )

    def _wait_for_shutdown(self):
        """Wait for shutdown signal and then shutdown all clients"""
        if self.shutdown_monitor:
            self.shutdown_monitor.join()  # Wait for shutdown monitor to finish

    def run(self):
        """Main listener loop with graceful shutdown support"""
        self.logger.info(
            f"Started consuming client notifications from queue: {self.queue_name}"
        )

        # Start shutdown monitoring thread with callback
        self.shutdown_monitor = ShutdownMonitor(
            shutdown_queue=self.shutdown_queue,
            shutdown_callback=self._handle_shutdown_signal,
        )
        self.shutdown_monitor.start()

        try:
            self.middleware.basic_consume(
                queue_name=self.queue_name, callback_function=self._handle_message
            )
            self.middleware.start_consuming()
        except Exception as e:
            if not self.shutdown_initiated:
                self.logger.error(f"Error in listener loop: {e}")
        finally:
            self._wait_for_shutdown()
            self.logger.info("Listener shutdown completed")

    def _handle_message(self, ch, method, properties, body):
        """Launch a ClientManager process for each new client notification (all logic inside ClientManager)."""
        try:
            self.logger.info("Received new client connection notification")

            # Only ack/nack if channel is open
            def safe_ack():
                if hasattr(ch, "is_open") and ch.is_open:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    self.logger.warning(
                        "Channel already closed, cannot ack notification message."
                    )

            def safe_nack():
                if hasattr(ch, "is_open") and ch.is_open:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                else:
                    self.logger.warning(
                        "Channel already closed, cannot nack notification message."
                    )

            if self.shutdown_initiated:
                self.logger.info(
                    "Shutdown already initiated, ignoring new client notification"
                )
                safe_nack()
                return

            # Create a shutdown queue for this client manager
            client_shutdown_queue = Queue()

            # ClientManager will create its own RabbitMQ connection
            client_manager = ClientManager(
                message_data=(properties, body),
                middleware_config=self.middleware.config,
                shutdown_queue=client_shutdown_queue,
                remove_handler_callback=self._remove_handler,
                add_client_callback=self._add_client,
            )

            client_manager.start()

            # ACK the notification message only if channel is open
            safe_ack()

            # Double-check for shutdown signal after starting the client manager
            if self.shutdown_initiated:
                client_shutdown_queue.put(None)  # Signal shutdown
                client_manager.join()
                return
        except Exception as e:
            self.logger.error(f"Error handling new client message: {e}")
            safe_nack()

    def _remove_handler(self, client_id: str):
        """Remove a finished client manager from the active clients dict"""
        with self._clients_lock:
            if client_id in self._active_clients:
                del self._active_clients[client_id]
                self.logger.info(f"Removed ClientManager for client {client_id}")

    def _add_client(self, client_id: str, handler: ClientManager):
        """Add a new client manager to the active clients dict"""
        with self._clients_lock:
            if client_id not in self._active_clients:
                self._active_clients[client_id] = handler
                self.logger.info(f"Added ClientManager for client {client_id}")
