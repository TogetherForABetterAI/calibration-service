import logging
import threading
from typing import Dict
from multiprocessing import Queue
from lib.config import CONNECTION_QUEUE_NAME, COORDINATOR_EXCHANGE
from server.client_manager import ClientManager
import json


class Listener:
    def __init__(self, middleware, channel, max_clients, replica_id, logger=None):
        self.middleware = middleware
        self.middleware_config = (
            middleware.config
        )  # Store config to pass to child processes
        self.logger = logger or logging.getLogger("listener")
        self.max_clients = max_clients
        self.channel = channel
        self.consumer_tag = None
        self.replica_id = replica_id

        # Queue to receive removal requests from child processes
        self.remove_client_queue = Queue()
        self.clients_removed_queue = Queue()

        # Track active client manager processes
        self._active_clients: Dict[str, ClientManager] = {}
        self._clients_lock = threading.Lock()

        self.shutdown_lock = threading.Lock()
        self.shutdown_initiated = False

        # Removal monitor thread
        self.remove_client_monitor = None

        self.logger.info(f"Listener initialized for queue: {CONNECTION_QUEUE_NAME}")

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
                    self.clients_removed_queue.put(None)
                    break  # We send None to stop the thread
                self._remove_handler(client_id)
                self.clients_removed_queue.put(client_id)
            except Exception as e:
                self.logger.error(f"Error monitoring removals: {e}")
                continue


    def start_consumption(self):
        active_clients_count = 0
        with self._clients_lock:
            active_clients_count = len(self._active_clients)

        while active_clients_count < self.max_clients:
            self.middleware.start_consuming(
                self.channel
            )
            client_id = self.clients_removed_queue.get(
                block=True
            )
            if client_id is None:
                break 
        
            with self._clients_lock:
                active_clients_count = len(self._active_clients)
        

    
    def start(self):
        """Main listener loop with graceful shutdown support"""

        # Thread to remove clients from _active_clients
        self.remove_client_monitor = threading.Thread(target=self._monitor_removals)
        self.remove_client_monitor.start()

        try:
            self.consumer_tag = self.middleware.basic_consume(
                channel=self.channel,
                queue_name=CONNECTION_QUEUE_NAME,
                callback_function=self._handle_new_client,
            )
            self.start_consumption()

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

            # Chequeo si se llego al maximo de clientes
            active_clients_count = 0
            with self._clients_lock:
                active_clients_count = len(self._active_clients)

            if active_clients_count >= self.max_clients:
                self.middleware.unsuscribe_from_queue(self.channel, self.consumer_tag)
                self.middleware.stop_consuming(self.channel)
                data = { "replica_id": self.replica_id }
                self.middleware.basic_send(
                    self.channel,
                    COORDINATOR_EXCHANGE,
                    "coordinator-scale",
                    json.dumps(data).encode("utf-8"),
                )                
                self.logger.info(f"Max clients reached ({self.max_clients}). Pausing consumption.")

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
