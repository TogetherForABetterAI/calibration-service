import logging
import threading
from time import time
from typing import Dict
from multiprocessing import Queue
from lib.config import CONNECTION_QUEUE_NAME, COORDINATOR_EXCHANGE
from server.client_manager import ClientManager
import json


class Listener:
    def __init__(
        self,
        middleware,
        channel,
        upper_bound_clients,
        lower_bound_clients,
        replica_id,
        replica_timeout_seconds,
        master_replica_id=1,
        initial_timeout=30,
        logger=None,
    ):
        self.middleware = middleware
        self.middleware_config = middleware.config
        self.logger = logger or logging.getLogger("Listener")

        # Configuración de límites
        self.upper_bound_clients = upper_bound_clients
        self.lower_bound_clients = lower_bound_clients
        self.replica_id = replica_id
        self.replica_timeout_seconds = replica_timeout_seconds
        self.initial_timeout = initial_timeout
        self.master_replica_id = master_replica_id

        self.channel = channel
        self.consumer_tag = None
        self.lower_bound_reached = False


        # Control de procesos cliente
        self._active_clients: Dict[str, ClientManager] = {}
        self._clients_lock = threading.Lock()

        # Control de apagado seguro
        self.shutdown_lock = threading.Lock()
        self.shutdown_initiated = False

        # Colas para comunicación entre hilos
        self.remove_client_queue = Queue()
        self.clients_removed_queue = Queue()
        self.lower_bound_reached_queue = Queue()

        # Thread de monitoreo
        self.remove_client_monitor = None
        self.lower_bound_reached_thread = None

        self.logger.info(f"Listener initialized for queue: {CONNECTION_QUEUE_NAME}")

    def _shutdown_all_clients(self):
        """Finaliza todos los procesos ClientManager activos."""
        self.logger.info("Shutting down all client managers...")
        with self._clients_lock:
            for client_id, handler in list(self._active_clients.items()):
                if handler.is_alive():
                    try:
                        handler.terminate()
                        handler.join(timeout=5)
                    except Exception as e:
                        self.logger.error(f"Error shutting down {client_id}: {e}")
                else:
                    self.logger.debug(f"ClientManager {client_id} already stopped")

    def _monitor_removals(self):
        """Monitor the removal queue and remove finished clients from _active_clients"""
        while True:
            try:
                client_id = self.remove_client_queue.get(
                    block=True
                )  # Block until a message is available
                if client_id is None:
                    self.clients_removed_queue.put(None)
                    self.lower_bound_reached_queue.put(None)
                    break 

                self._remove_client(client_id)
                self.clients_removed_queue.put(client_id)
                self.lower_bound_reached_queue.put(client_id)
            except Exception as e:
                self.logger.error(f"Error monitoring removals: {e}")
                continue


    def _get_active_clients_count(self) -> int:
        """Obtener el número actual de clientes activos (thread-safe)."""
        with self._clients_lock:
            return len(self._active_clients)

    def start_consumption(self):
        while True:
            if self._get_active_clients_count() >= self.upper_bound_clients:
                break

            self.middleware.start_consuming(self.channel)
            client_id = self.clients_removed_queue.get(block=True)
            if client_id is None:
                break 
        

    def initial_lower_bound_checker(self):
        """Check if the number of active clients is below half the max_clients after initial timeout"""
        try:
            time.sleep(self.initial_timeout)
            with self._clients_lock:
                if not self.lower_bound_reached:
                    self.stop_consuming()
                    self.clients_removed_queue.put(None)
                    return

        except Exception as e:
            self.logger.error(f"Error in initial lower bound check: {e}")
            return
        
    def lower_bound_checker(self):
        """Check if the number of active clients has dropped below half the max_clients"""
        self.initial_lower_bound_checker()

        while True:
            try:
                client_id = self.lower_bound_reached_queue.get(
                    block=True
                )  
                if client_id is None:
                    break  

                time.sleep(self.replica_timeout_seconds)
                active_clients_count = self._get_active_clients_count()

                if active_clients_count <= self.lower_bound_clients:
                    self.stop_consuming()
                    self.clients_removed_queue.put(None)
                    break
            
            except Exception as e:
                self.logger.error(f"Error in lower bound checker: {e}")
                continue        

    def start(self):
        """Main listener loop with graceful shutdown support"""

        # Thread to remove clients from _active_clients
        self.remove_client_monitor = threading.Thread(target=self._monitor_removals)
        self.remove_client_monitor.start()

        if self.replica_id != self.master_replica_id:
            self.lower_bound_reached_thread = threading.Thread(target=self.lower_bound_checker)
            self.lower_bound_reached_thread.start()
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
            active_clients_count = self._get_active_clients_count()

            if active_clients_count >= self.lower_bound_clients and not self.lower_bound_reached:
                self.lower_bound_reached = True
                
            if active_clients_count >= self.max_clients:
                self.middleware.stop_consuming(self.channel)
                self._notify_coordinator_scale()


            client_manager.start()

        except Exception as e:
            self.logger.error(f"Error handling new client message: {e}")
            raise e  # Requeue the message

    def _notify_coordinator_scale(self):
        """Notify the coordinator exchange to scale up replicas."""
        try:
            message = {
                "replica_id": self.replica_id,
                "action": "scale_up",
                "timestamp": time(),
            }
            message_body = json.dumps(message).encode("utf-8")
            routing_key = "scale.up"

            self.middleware.publish_message(
                exchange_name=COORDINATOR_EXCHANGE,
                routing_key=routing_key,
                message_body=message_body,
            )
            self.logger.info(
                f"Sent scale up notification to coordinator from replica {self.replica_id}"
            )
        except Exception as e:
            self.logger.error(f"Failed to notify coordinator for scaling: {e}")


    def _remove_client(self, client_id: str):
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
        if self.replica_id != self.master_replica_id:
            self.lower_bound_reached_thread.join()

        self._shutdown_all_clients()
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()
        self.logger.info("Listener shutdown completed")

    def stop_consuming(self):
        """Signal to stop consuming messages and initiate shutdown."""
        with self.shutdown_lock:
            if self.shutdown_initiated:
                return 
            self.shutdown_initiated = True

        self.remove_client_queue.put(None)
        if self.remove_client_monitor:
            self.remove_client_monitor.join()

        if self.lower_bound_reached_thread:
            self.lower_bound_reached_thread.join()

        self._shutdown_all_clients()
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()            
        self.middleware.stop_consuming(self.channel)
