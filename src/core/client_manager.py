import logging
from typing import Dict, Optional

from mlflow import MlflowClient
from middleware.middleware import Middleware, middleware_connect
from service.client_processor import ClientProcessor
from multiprocessing import Process, Lock

class ClientManager:
    def __init__(self):
        self._clients: Dict[str, ClientProcessor] = {}
        self._clients_processes: Dict[str, Process] = {}
        self._lock = Lock()
        self._rabbitmq_channels = {}

    def register_client(
        self, client_id
    ) :
        """
        Args:
            client_id: Unique identifier for the client
            outputs_queue_calibration: Queue name for calibration messages
            inputs_queue_calibration: Queue name for inter-connection messages
        """
        client = MlflowClient()
        Middleware.create_exchange("calibration_exchange")

        with self._lock:
            if client_id in self._clients:
                raise Exception(f"Client {client_id} is already registered")

            try:
                conn = middleware_connect()
            except Exception as e:
                logging.error(f"Failed to connect to RabbitMQ: {e}")
                raise e
            
            channel = conn.channel()
            self._rabbitmq_channels[client_id] = channel

            try:
                client_processor = ClientProcessor(
                client_id=client_id,
                middleware=Middleware(channel=channel),
                mlflow_client=client
                )
            except Exception as e:
                logging.error(f"Failed to initialize ClientProcessor for client {client_id}: {e}")
                raise e
            
            process = Process(
                target=client_processor.start_processing,
                name=f"client-{client_id}",
                daemon=True,
            )

            self._clients[client_id] = client_processor
            self._clients_processes[client_id] = process
            process.start()

            #TODO: Borrar colas del cliente y channel al finalizar la evaluacion metrologica



    def unregister_client(self, client_id: str) -> bool:
        """
        Args:
            client_id: Unique identifier for the client

        Returns:
            bool: True if client was successfully unregistered, False otherwise
        """
        with self._lock:
            if client_id not in self._clients:
                logging.warning(f"Client {client_id} is not registered")
                return False

            try:
                # Stop the client processor
                client_processor = self._clients[client_id]
                client_processor.stop_processing()

                process = self._clients_processes[client_id]
                process.join()

                # Clean up
                del self._clients[client_id]
                del self._clients_processes[client_id]

                logging.info(f"Successfully unregistered client {client_id}")
                return True

            except Exception as e:
                logging.error(f"Failed to unregister client {client_id}: {e}")
                return False

    def get_client(self, client_id: str) -> Optional[ClientProcessor]:
        """Get a client processor by ID."""
        with self._lock:
            return self._clients.get(client_id)

    def get_active_clients(self) -> list:
        """Get list of active client IDs."""
        with self._lock:
            return list(self._clients.keys())

    def shutdown_all(self):
        """Shutdown all client processors."""
        with self._lock:
            for client_id in list(self._clients.keys()):
                self.unregister_client(client_id)
