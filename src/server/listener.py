import logging
import os
import signal
import threading
import time
from typing import Dict, Optional
from multiprocessing import Queue
from lib.config import CONNECTION_QUEUE_NAME, COORDINATOR_EXCHANGE
from server.client_manager import ClientManager
import json

from src.lib.inputs_format_parser import parse_inputs_format
from src.middleware.middleware import Middleware


class Listener:
    def __init__(
        self,
        middleware, 
        channel, 
        config,
        cm_middleware_factory, 
        report_builder_factory, 
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

        self.logger = logger or logging.getLogger("listener")
        self.channel = channel
        self.consumer_tag = None
        self.database = database

        self.config = config
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
        
        self.logger.info(f"Listener initialized for queue: {CONNECTION_QUEUE_NAME}")


         
    def _monitor_removals(self):
        """Monitor the removal queue and remove finished clients from _active_clients"""
        while True:
            try:
                user_id = self.clients_to_remove_queue.get(block=True)
                if user_id is None:
                    break

                self._remove_client(user_id)
            except Exception as e:
                self.logger.error(f"Error in _monitor_removals: {e}")
                continue

    def _shutdown_all_clients(self):
        """Finaliza todos los procesos ClientManager activos."""
        self.logger.info("Shutting down all client managers...")
        with self._active_clients_lock:
            for user_id, handler in list(self._active_clients.items()):
                if handler.is_alive():
                    try:
                        handler.terminate()
                        handler.join(timeout=5)
                    except Exception as e:
                        self.logger.error(f"Error shutting down {user_id}: {e}")
                else:
                    self.logger.debug(f"ClientManager {user_id} already stopped")

    def _get_active_clients_count(self) -> int:
        """Obtener el número actual de clientes activos (thread-safe)."""
        with self._active_clients_lock:
            return len(self._active_clients)


    def start(self):
        """Main listener loop with graceful shutdown support"""
        logging.info("Listener starting consumption loop...")
        self.remove_client_monitor = threading.Thread(target=self._monitor_removals)
        self.remove_client_monitor.start()

        if not self.shutdown_initiated: 
            self.middleware.start_consuming(self.channel)
 
        if self.shutdown_initiated:
            self.finish()
            return
            
        logging.info("Listener stopping consumption...")
        self.finish()
    
    def _handle_new_client(self, ch, method, properties, body):
        """Launch a ClientManager process for each new client notification (all logic inside ClientManager)."""
        self.logger.info("Received new client connection notification")

        notification = json.loads(body.decode("utf-8"))
        user_id = notification.get("user_id")
        session_id = notification.get("session_id")
        inputs_format = parse_inputs_format(notification.get("inputs_format"))

        if not user_id:
            self.logger.info(
                f"Client notification missing user_id: {notification}"
            )
            return  
        

        if not self.middleware.is_running():
            self.logger.info(
                f"Shutdown initiated, ignoring new client {user_id}"
            )
            return
        
        client_manager = ClientManager(
            user_id=user_id,
            session_id=session_id,
            middleware=self.cm_middleware_factory(self.middleware_config),
            clients_to_remove_queue=self.clients_to_remove_queue,
            report_builder=self.report_builder_factory(user_id=user_id),
            config=self.config,
            database=self.database,
            inputs_format=inputs_format,
        )
        logging.info(f"Created ClientManager for client {user_id}")
        self._add_client(user_id, client_manager)

        logging.info(f"Starting ClientManager for client {user_id}")
        client_manager.start()

    def _remove_client(self, user_id: str):
        """Remove a finished client manager from the active clients dict"""
        with self._active_clients_lock:
            if user_id in self._active_clients:
                del self._active_clients[user_id]
                self.logger.info(f"Removed ClientManager for client {user_id}")

    def _add_client(self, user_id: str, handler: ClientManager):
        """Add a new client manager to the active clients dict"""
        with self._active_clients_lock:
            if user_id not in self._active_clients:
                self._active_clients[user_id] = handler
                self.logger.info(f"Added ClientManager for client {user_id}")

    def terminate_all_clients(self):
        """Join all active ClientManager processes."""
        self.logger.info("Joining all client managers...")
        active_clients = list(self._active_clients.items())
        for (
            user_id,
            handler,
        ) in active_clients:
            if handler.is_alive():
                try:
                    os.kill(handler.pid, signal.SIGTERM)
                    logging.info(f"Terminated ClientManager for client {user_id}...")
                    handler.join()
                except Exception as e:
                    self.logger.error(
                        f"Error shutting down ClientManager {user_id}: {e}"
                    )

    def finish(self):
        """Finish listener operations before shutdown."""
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()


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
        
