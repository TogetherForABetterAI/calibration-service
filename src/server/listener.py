import logging
import threading
from typing import Dict
from multiprocessing import Queue
from lib.config import CONNECTION_QUEUE_NAME, CONNECTION_EXCHANGE
from server.client_manager import ClientManager
import json


class Listener:
    def __init__(self, middleware, channel, logger=None):
        super().__init__()  # Initialize Thread base class
        self.middleware = middleware
        self.middleware_config = (
            middleware.config
        )  # Store config to pass to child processes
        self.logger = logger or logging.getLogger("listener")
        self.queue_name = CONNECTION_QUEUE_NAME
        self.connection_exchange = CONNECTION_EXCHANGE
        self.channel = channel

        # Queue to receive removal requests from child processes
        self.remove_client_queue = Queue()

        # Track active client manager processes
        self._active_clients: Dict[str, ClientManager] = {}
        self._clients_lock = threading.Lock()

        self.shutdown_lock = threading.Lock()
        self.shutdown_initiated = False

        # Removal monitor thread
        self.remove_client_monitor = None

        self.logger.info(f"Listener initialized for queue: {self.queue_name}")

    def _shutdown_all_clients(self):
        """Terminate all active ClientManager processes."""
        self.logger.info("Shutting down all client managers...")

        for (
            client_id,
            handler,
        ) in self._active_clients.items():  # No need to lock _active_clients here
            if handler.is_alive():
                try:
                    handler.terminate()  # Send SIGTERM to the process
                    handler.join()
                except Exception as e:
                    self.logger.error(
                        f"Error shutting down ClientManager {client_id}: {e}"
                    )
            else:
                self.logger.info(f"ClientManager {client_id} already stopped")

    def _monitor_removals(self):
        """Monitor the removal queue and remove finished clients from _active_clients"""
        while True:
            try:
                client_id = self.remove_client_queue.get(
                    block=True
                )  # Block until a message is available
                if client_id is None:
                    break  # We send None to stop the thread
                self._remove_handler(client_id)
            except:
                continue

    def start(self):
        """Main listener loop with graceful shutdown support"""

        # Thread to remove clients from _active_clients
        self.remove_client_monitor = threading.Thread(target=self._monitor_removals)
        self.remove_client_monitor.start()

        try:
            self.middleware.basic_consume(
                channel=self.channel,
                queue_name=self.queue_name,
                callback_function=self._handle_new_client,
            )
            self.middleware.start_consuming(
                self.channel
            )  # This will block until stop_consuming() is called
        except Exception as e:
            with self.shutdown_lock:
                if not self.shutdown_initiated:
                    self.logger.error(f"Error in listener loop: {e}")
        finally:
            self.shutdown()

    def _handle_new_client(self, ch, method, properties, body):
        """Launch a ClientManager process for each new client notification (all logic inside ClientManager)."""
        try:
            self.logger.info("Received new client connection notification")

            with self.shutdown_lock:
                if self.shutdown_initiated:
                    raise Exception(
                        "Shutdown in progress, not accepting new clients"
                    )  # Requeue the message

            # Parse client_id from message before creating process
            notification = json.loads(body.decode("utf-8"))
            client_id = notification.get("client_id")
            if not client_id:
                self.logger.info(
                    f"Client notification missing client_id: {notification}"
                )
                return  # Ack the message to remove it from the queue

            # ClientManager will create its own RabbitMQ connection
            client_manager = ClientManager(
                client_id=client_id,
                middleware_config=self.middleware_config,
                remove_client_queue=self.remove_client_queue,
            )

            # Add to active clients before starting the process
            self._add_client(client_id, client_manager)

            client_manager.start()

        except Exception as e:
            self.logger.error(f"Error handling new client message: {e}")
            raise e  # Requeue the message

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

    def shutdown(self):
        """Gracefully shutdown listener and all resources."""
        self.remove_client_queue.put(None)  # Signal to stop
        self.remove_client_monitor.join()  # Wait to finish
        self._shutdown_all_clients()
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()
        self.logger.info("Listener shutdown completed")

    def stop_consuming(self):
        """Signal to stop consuming messages and initiate shutdown."""
        with self.shutdown_lock:
            self.shutdown_initiated = True
        self.middleware.stop_consuming(self.channel)
