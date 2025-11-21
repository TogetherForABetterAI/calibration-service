import logging
import os
import signal
import threading
import time
from typing import Dict
from multiprocessing import Queue
from lib.config import CONNECTION_QUEUE_NAME, COORDINATOR_EXCHANGE
from server.client_manager import ClientManager
import json

from src.middleware.middleware import Middleware


class Listener:
    def __init__(
        self,
        middleware, 
        channel, 
        upper_bound_clients, 
        lower_bound_clients,  
        cm_middleware_factory,         
        replica_id,
        replica_timeout_seconds,
        report_builder_factory, 
        master_replica_id=1,
        initial_timeout=30, 
        database=None,
        logger=None,
    ):
        self.middleware = middleware
        self.middleware_config = (
            middleware.config
        )  # Store config to pass to child processes
        self.middleware.basic_consume(
            channel=channel,
            queue_name=CONNECTION_QUEUE_NAME,
            callback_function=self._handle_new_client,
        )
        self.upper_bound_clients = upper_bound_clients
        self.lower_bound_clients = lower_bound_clients
        self.logger = logger or logging.getLogger("listener")
        self.channel = channel
        self.consumer_tag = None

        self.lower_bound_reached = False
        self.lower_bound_reached_lock = threading.Lock()
        self.lower_bound_event = threading.Event()

        
        # Configuración de límites
        self.replica_id = replica_id
        self.replica_timeout_seconds = replica_timeout_seconds
        self.initial_timeout = initial_timeout
        self.master_replica_id = master_replica_id

        # Client manager factory
        self.cm_middleware_factory = cm_middleware_factory
        self.report_builder_factory = report_builder_factory

        # Control de procesos cliente
        self._active_clients: Dict[str, ClientManager] = {}
        self._active_clients_lock = threading.Lock()

        # Control de apagado seguro
        self.shutdown_initiated = False

        # Colas para comunicación entre hilos
        self.clients_to_remove_queue = Queue()
        self.clients_removed_queue = Queue()
        self.lower_bound_reached_queue = Queue()

        # Thread de monitoreo
        self.remove_client_monitor = None
        self.lower_bound_reached_thread = None

        self.logger.info(f"Listener initialized for queue: {CONNECTION_QUEUE_NAME}")

    def _monitor_removals(self):
        """Monitor the removal queue and remove finished clients from _active_clients"""
        while True:
            try:
                client_id = self.clients_to_remove_queue.get(block=True)
                if client_id is None:
                    self.clients_removed_queue.put(None)
                    self.lower_bound_reached_queue.put(None)
                    self.lower_bound_event.set()  # 🔹 desbloquear si estamos esperando
                    break

                self._remove_handler(client_id)
                self.clients_removed_queue.put(client_id)

                active = self._get_active_clients_count()
                if active < self.lower_bound_clients:
                    self.lower_bound_reached_queue.put(client_id)
                    self.lower_bound_event.set()  # 🔹 señalar que bajó del límite

            except Exception as e:
                self.logger.error(f"Error in _monitor_removals: {e}")
                continue

    def _shutdown_all_clients(self):
        """Finaliza todos los procesos ClientManager activos."""
        self.logger.info("Shutting down all client managers...")
        with self._active_clients_lock:
            for client_id, handler in list(self._active_clients.items()):
                if handler.is_alive():
                    try:
                        handler.terminate()
                        handler.join(timeout=5)
                    except Exception as e:
                        self.logger.error(f"Error shutting down {client_id}: {e}")
                else:
                    self.logger.debug(f"ClientManager {client_id} already stopped")

    def _wait_for_lower_bound(self, timeout: float) -> bool:
        """
        Espera a que el número de clientes activos supere el lower bound.
        Retorna True si se cumple dentro del timeout, False si no.
        """
        start = time.time()
        while time.time() - start < timeout:
            if self.shutdown_initiated:
                return False

            if self._get_active_clients_count() >= self.lower_bound_clients:
                return True

            if self.lower_bound_event.wait(timeout=1.0):
                self.lower_bound_event.clear()

        return False

    def initial_lower_bound_reached(self):
        """Verifica si se alcanza el lower bound inicial dentro del timeout."""
        try:
            if self._wait_for_lower_bound(self.initial_timeout):
                with self.lower_bound_reached_lock:
                    self.lower_bound_reached = True
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(f"Error in initial lower bound check: {e}")
            return False


    def _get_active_clients_count(self) -> int:
        """Obtener el número actual de clientes activos (thread-safe)."""
        with self._active_clients_lock:
            return len(self._active_clients)

    def start_consumption(self):
        while True:
            if self._get_active_clients_count() >= self.upper_bound_clients:
                break

            self.middleware.start_consuming(self.channel)

            while True:
                client_id = self.clients_removed_queue.get(block=True)
                if self.clients_removed_queue.empty():
                   break

            if self.shutdown_initiated or client_id is None:
                break

        
    def lower_bound_checker(self):
        """Monitorea si los clientes activos bajan del límite inferior."""
        if not self.initial_lower_bound_reached():
            self.logger.warning("Initial lower bound not reached, shutting down listener.")
            self.handle_sigterm()
            self.clients_removed_queue.put(None)
            return

        while not self.shutdown_initiated:
            try:
                self.logger.debug("Waiting for lower bound event...")
                if not self.lower_bound_event.wait(timeout=self.replica_timeout_seconds):
                    continue  # nadie bajó el límite aún

                self.lower_bound_event.clear()
                active = self._get_active_clients_count()
                if active <= self.lower_bound_clients:
                    self.logger.warning(f"Active clients below lower bound ({active}). Initiating shutdown.")
                    self.handle_sigterm()
                    self.clients_removed_queue.put(None)
                    break

            except Exception as e:
                self.logger.error(f"Error in lower bound checker: {e}")
                continue     

    def start(self):
        """Main listener loop with graceful shutdown support"""
        logging.info("Listener starting consumption loop...")
        self.remove_client_monitor = threading.Thread(target=self._monitor_removals)
        self.remove_client_monitor.start()

        if self.replica_id != self.master_replica_id:
            self.lower_bound_reached_thread = threading.Thread(target=self.lower_bound_checker)
            self.lower_bound_reached_thread.start()
            
        if not self.shutdown_initiated: 
            self.start_consumption()
 
        if self.shutdown_initiated:
            self.finish()
            return
            
        logging.info("Listener stopping consumption...")
        self.finish()
        

    def _handle_new_client(self, ch, method, properties, body):
        """Launch a ClientManager process for each new client notification (all logic inside ClientManager)."""
        self.logger.info("Received new client connection notification")

        notification = json.loads(body.decode("utf-8"))
        client_id = notification.get("client_id")

        if not client_id:
            self.logger.info(
                f"Client notification missing client_id: {notification}"
            )
            return  

        if not self.middleware.is_running():
            self.logger.info(
                f"Shutdown initiated, ignoring new client {client_id}"
            )
            return
        
        client_manager = ClientManager(
            client_id=client_id,
            middleware=self.cm_middleware_factory(self.middleware_config),
            clients_to_remove_queue=self.clients_to_remove_queue,
            report_builder=self.report_builder_factory(client_id=client_id),
            database=self.database,
        )
        logging.info(f"Created ClientManager for client {client_id}")
        self._add_client(client_id, client_manager)
        active_clients_count = self._get_active_clients_count()

        logging.info(f"Active clients count: {active_clients_count}")
        with self.lower_bound_reached_lock:
            if active_clients_count >= self.lower_bound_clients and not self.lower_bound_reached:
                logging.info("Lower bound of active clients reached.")
                self.lower_bound_reached = True
            
        if active_clients_count >= self.upper_bound_clients:
            logging.info("Upper bound of active clients reached, stopping consumption.")
            self.middleware.stop_consuming()
            self._notify_coordinator_scale()

        logging.info(f"Starting ClientManager for client {client_id}")
        client_manager.start()

     
    def _notify_coordinator_scale(self):
        """Notify the coordinator exchange to scale up replicas."""
        try:
            message = {
                "replica_id": self.replica_id,
                "action": "scale_up",
                "timestamp": time.time(),
            }
            message_body = json.dumps(message, ensure_ascii=False).encode("utf-8")
            routing_key = "scale.up"

            self.middleware.basic_send(
                channel=self.channel,
                exchange_name=COORDINATOR_EXCHANGE,
                routing_key=routing_key,
                body=message_body,
            )
            self.logger.info(
                f"Sent scale up notification to coordinator from replica {self.replica_id}"
            )
        except Exception as e:
            self.logger.error(f"Failed to notify coordinator for scaling: {e}")


    def _remove_client(self, client_id: str):
        """Remove a finished client manager from the active clients dict"""
        with self._active_clients_lock:
            if client_id in self._active_clients:
                del self._active_clients[client_id]
                self.logger.info(f"Removed ClientManager for client {client_id}")

    def _add_client(self, client_id: str, handler: ClientManager):
        """Add a new client manager to the active clients dict"""
        with self._active_clients_lock:
            if client_id not in self._active_clients:
                self._active_clients[client_id] = handler
                self.logger.info(f"Added ClientManager for client {client_id}")

    def _remove_handler(self, client_id: str):
        """Remove a finished client manager from the active clients dict"""
        with self._active_clients_lock:
            if client_id in self._active_clients:
                del self._active_clients[client_id]
                self.logger.info(f"Removed ClientManager for client {client_id}")

    def terminate_all_clients(self):
        """Join all active ClientManager processes."""
        self.logger.info("Joining all client managers...")
        active_clients = list(self._active_clients.items())
        for (
            client_id,
            handler,
        ) in active_clients:
            if handler.is_alive():
                try:
                    os.kill(handler.pid, signal.SIGTERM)
                    logging.info(f"Terminated ClientManager for client {client_id}...")
                    handler.join()
                except Exception as e:
                    self.logger.error(
                        f"Error shutting down ClientManager {client_id}: {e}"
                    )

    def finish(self):
        """Finish listener operations before shutdown."""
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()
        if self.replica_id != self.master_replica_id:
            self.lower_bound_reached_thread.join()


    def handle_sigterm(self):
        """Signal to stop consuming messages and initiate shutdown."""
        logging.info("Listener received SIGTERM, initiating shutdown...")
        self.shutdown_initiated = True
        if self.middleware.is_running():
            self.middleware.stop_consuming()

        if not self.middleware.on_callback():
            self.middleware.cancel_channel_consuming(self.channel)
        
        self.terminate_all_clients()
        self.clients_to_remove_queue.put(None)  
        self.remove_client_monitor.join()
        
