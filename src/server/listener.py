import logging
import threading
from threading import Thread
from typing import Dict, Tuple
from multiprocessing import Queue
from lib.constants import CONNECTION_QUEUE_NAME, CONNECTION_EXCHANGE
from server.client_manager import ClientManager
from server.shutdown_monitor import ShutdownMonitor
import json


class Listener(Thread):
    def __init__(self, middleware, logger=None):
        super().__init__()  # Initialize Thread base class
        self.middleware = middleware
        self.logger = logger or logging.getLogger("listener")
        self.queue_name = CONNECTION_QUEUE_NAME
        self.connection_exchange = CONNECTION_EXCHANGE

        # Queue to receive shutdown signal
        self.shutdown_queue = Queue()

        # Queue to receive removal requests from child processes
        self.remove_client_queue = Queue()

        # Track active client manager processes
        self._active_clients: Dict[str, Tuple[ClientManager, Queue]] = {}
        # client_id -> (ClientManager, shutdown_queue)
        self._clients_lock = threading.Lock()

        # Shutdown monitor thread
        self.shutdown_monitor = None
        self.shutdown_initiated = False

        # Removal monitor thread
        self.remove_client_monitor = None

        self.logger.info(f"Listener initialized for queue: {self.queue_name}")

    def _handle_shutdown_signal(self):
        """Callback function called by ShutdownMonitor when shutdown signal is received"""
        self.logger.info("Shutdown callback invoked, initiating graceful shutdown...")
        self.shutdown_initiated = True

        self.middleware.stop_consuming()

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
            else:
                self.logger.info(f"ClientManager {client_id} already stopped")

    def _wait_for_shutdown(self):
        """Wait for shutdown signal and then shutdown all clients"""
        if self.shutdown_monitor:
            self.shutdown_monitor.join()  # Wait for shutdown monitor to finish

    def _monitor_removals(self):
        """Monitor the removal queue and remove finished clients from _active_clients"""
        while not self.shutdown_initiated:
            try:
                client_id = self.remove_client_queue.get(
                    block=True
                )  # Block until a message is available
                if client_id is None:
                    break  # We send None to stop the thread
                self._remove_handler(client_id)
            except:
                continue

    def run(self):
        """Main listener loop with graceful shutdown support"""

        # Thread to remove clients from _active_clients
        self.remove_client_monitor = threading.Thread(
            target=self._monitor_removals, daemon=False
        )
        self.remove_client_monitor.start()

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
            self.remove_client_queue.put(None)  # Signal to stop
            self.remove_client_monitor.join()  # Wait to finish
            self._shutdown_all_clients()
            self.middleware.close()  # Close middleware connection
            self.logger.info("Listener shutdown completed")

    def _handle_message(self, ch, method, properties, body):
        """Launch a ClientManager process for each new client notification (all logic inside ClientManager)."""
        try:
            self.logger.info("Received new client connection notification")

            if self.shutdown_initiated:
                raise Exception("Shutdown in progress")  # Requeue the message

            # Parse client_id from message before creating process

            notification = json.loads(body.decode("utf-8"))
            client_id = notification.get("client_id")
            if not client_id:
                self.logger.info(
                    f"Client notification missing client_id: {notification}"
                )
                return  # Ack the message to remove it from the queue

            # Create a shutdown queue for this client manager
            client_shutdown_queue = Queue()

            # ClientManager will create its own RabbitMQ connection
            client_manager = ClientManager(
                client_id=client_id,
                message_data=(properties, body),
                middleware_config=self.middleware.config,
                shutdown_queue=client_shutdown_queue,
                remove_client_queue=self.remove_client_queue,
            )

            # Add to active clients BEFORE starting the process
            self._add_client(client_id, (client_manager, client_shutdown_queue))

            client_manager.start()

            # Double-check for shutdown signal after starting the client manager
            if self.shutdown_initiated:
                client_shutdown_queue.put(None)  # Signal shutdown
                client_manager.join()
                return
        except Exception as e:
            self.logger.error(f"Error handling new client message: {e}")
            raise e  # Requeue the message

    def _remove_handler(self, client_id: str):
        """Remove a finished client manager from the active clients dict"""
        with self._clients_lock:
            if client_id in self._active_clients:
                del self._active_clients[client_id]
                self.logger.info(f"Removed ClientManager for client {client_id}")

    def _add_client(self, client_id: str, client_tuple: Tuple[ClientManager, Queue]):
        """Add a new client manager to the active clients dict"""
        with self._clients_lock:
            if client_id not in self._active_clients:
                self._active_clients[client_id] = client_tuple
                self.logger.info(f"Added ClientManager for client {client_id}")
