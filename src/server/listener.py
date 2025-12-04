import logging
import threading
from typing import Dict
from multiprocessing import Queue
from lib.config import CONNECTION_QUEUE_NAME
from server.client_manager import ClientManager
import json
import pika.exceptions
from src.lib.inputs_format_parser import parse_inputs_format


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
            consumer_tag=config.pod_name
        )

        self.logger = logger or logging.getLogger("listener")
        self.channel = channel
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

    
    def start(self):
        """Main listener loop with graceful shutdown support"""
        self.logger.info("Listener starting consumption loop...")
        try:
            self.remove_client_monitor = threading.Thread(target=self._monitor_removals)
            self.remove_client_monitor.start()
            while not self.shutdown_initiated:
                try:
                    if not self.shutdown_initiated: 
                        self.middleware.start_consuming(self.channel)
                except pika.exceptions.AMQPConnectionError as e:
                    self.logger.error(f"AMQP Connection error in Listener: {e}")
                    if not self.shutdown_initiated:
                        self.reconnect_to_middleware()
                except Exception as e:
                    self.logger.error(f"Error in Listener consumption loop: {e}")
                    break
        except Exception as e:
            self.logger.error(f"Error starting listener: {e}")
        finally:
            self.clients_to_remove_queue.put(None)  # Signal monitor to stop
            self.remove_client_monitor.join()
            self.finish()
            self.logger.info("Listener stopped consumption...")


    def reconnect_to_middleware(self):
        self.middleware.connect()
        self.channel = self.middleware.create_channel(prefetch_count=self.config.upper_bound_clients)
        self.middleware.basic_consume(
            channel=self.channel,
            queue_name=CONNECTION_QUEUE_NAME,
            callback_function=self._handle_new_client,
            consumer_tag=self.config.pod_name
            )
    
    # Callback for new client notifications
    def _handle_new_client(self, ch, method, properties, body):
        """Launch a ClientManager process for each new client notification (all logic inside ClientManager)."""
        self.logger.info("Received new client connection notification")

        notification = json.loads(body.decode("utf-8"))
        user_id = notification.get("user_id")
        session_id = notification.get("session_id")
        inputs_format = parse_inputs_format(notification.get("inputs_format"))

        if not user_id or not session_id:
            self.logger.info(
                f"Client notification missing fields: {notification}"
            )
            raise ValueError("Client notification missing fields") # dont requeue
        

        if not self.middleware.is_running():
            self.logger.info(
                f"Shutdown initiated, ignoring new client {user_id}"
            )
            return # the message will be requeued since we didnt ack it
        
        client_manager = ClientManager(
            ch=ch,
            delivery_tag=method.delivery_tag,
            user_id=user_id,
            session_id=session_id,
            middleware=self.cm_middleware_factory(self.middleware_config),
            clients_to_remove_queue=self.clients_to_remove_queue,
            report_builder=self.report_builder_factory(user_id=user_id),
            config=self.config,
            database=self.database,
            inputs_format=inputs_format,
        )
        self.logger.info(f"Created ClientManager for client {user_id}")
        self._add_client(user_id, client_manager)

        self.logger.info(f"Starting ClientManager for client {user_id}")
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


    def _terminate_all_clients(self):
        """Join all active ClientManager processes."""
        self.logger.info("Joining all client managers...")
        with self._active_clients_lock:
            active_clients = list(self._active_clients.items())
        for (
            user_id,
            handler,
        ) in active_clients:
            if handler.is_alive():
                try:
                    handler.terminate()
                    handler.join()
                    self.logger.info(f"Terminated ClientManager for client {user_id}...")
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
        self.logger.info("Listener received SIGTERM, initiating shutdown...")
        self.shutdown_initiated = True
        self.middleware.stop_consuming(self.channel) # stop consuming new messages
        self.middleware.handle_sigterm() # in case its trying to reconnect
        
        self._terminate_all_clients()
        self.clients_to_remove_queue.put(None)  
        self.remove_client_monitor.join()
        
