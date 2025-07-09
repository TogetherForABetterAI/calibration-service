import threading
import logging
from typing import Dict, Optional
from middleware.middleware import Middleware
from service.client_processor import ClientProcessor


class ClientManager:
    def __init__(self):
        self._clients: Dict[str, ClientProcessor] = {}
        self._client_threads: Dict[str, threading.Thread] = {}
        self._lock = threading.Lock()

    def register_client(self, client_id: str, routing_key: str) -> bool:
        """
        Register a new client and start its processing thread.

        Args:
            client_id: Unique identifier for the client
            routing_key: Routing key for both dataset and calibration exchanges

        Returns:
            bool: True if client was successfully registered, False otherwise
        """
        with self._lock:
            if client_id in self._clients:
                logging.warning(f"Client {client_id} is already registered")
                return False

            try:
                # Create a new middleware connection for this client
                middleware = Middleware()

                # Create client processor
                client_processor = ClientProcessor(
                    client_id=client_id,
                    routing_key=routing_key,
                    middleware=middleware,
                )

                # Create and start the processing thread
                thread = threading.Thread(
                    target=client_processor.start_processing,
                    name=f"client-{client_id}",
                    daemon=True,
                )

                self._clients[client_id] = client_processor
                self._client_threads[client_id] = thread
                thread.start()

                logging.info(
                    f"Successfully registered client {client_id} with routing_key: {routing_key}"
                )
                return True

            except Exception as e:
                logging.error(f"Failed to register client {client_id}: {e}")
                return False

    def unregister_client(self, client_id: str) -> bool:
        """
        Unregister a client and stop its processing thread.

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

                # Wait for thread to finish (with timeout)
                thread = self._client_threads[client_id]
                thread.join(timeout=5.0)

                # Clean up
                del self._clients[client_id]
                del self._client_threads[client_id]

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
