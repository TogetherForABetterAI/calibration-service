import threading
import logging
import time
from typing import Dict, Optional
import numpy as np
from google.protobuf.message import Message
from proto import calibration_pb2, dataset_pb2
from service.mlflow_logger import MlflowLogger
from middleware.middleware import Middleware
from config.settings import settings


class ClientProcessor:
    def __init__(
        self,
        client_id: str,
        routing_key: str,
        middleware: Middleware,
    ):
        self.client_id = client_id
        self.routing_key = routing_key
        self.middleware = middleware

        # Threading control
        self._running = False
        self._lock = threading.Lock()

        # Data coordination
        self._batches: Dict[int, Dict] = {}
        self._batches_lock = threading.Lock()

        # MLflow logger for this client
        self._mlflow_logger = MlflowLogger(client_id=client_id)

        logging.info(f"Initialized ClientProcessor for client {client_id}")

    def start_processing(self):
        """Start processing messages from both exchanges using the routing key."""
        with self._lock:
            if self._running:
                logging.warning(
                    f"ClientProcessor for {self.client_id} is already running"
                )
                return

            self._running = True
            logging.info(f"Starting processing for client {self.client_id}")

        try:
            # Declare queue using routing key as queue name
            logging.info(f"Declaring queue: {self.routing_key}")
            self.middleware._channel.queue_declare(
                queue=self.routing_key, durable=True, auto_delete=False
            )
            logging.info(f"Successfully declared queue: {self.routing_key}")

            # Bind queue to both exchanges using the routing key
            logging.info(
                f"Binding queue {self.routing_key} to exchange {settings.INTER_CONNECTION_EXCHANGE}"
            )
            self.middleware._channel.queue_bind(
                queue=self.routing_key,
                exchange=settings.INTER_CONNECTION_EXCHANGE,
                routing_key=self.routing_key,
            )
            logging.info(
                f"Successfully bound queue {self.routing_key} to {settings.INTER_CONNECTION_EXCHANGE}"
            )

            logging.info(
                f"Binding queue {self.routing_key} to exchange {settings.CALIBRATION_EXCHANGE}"
            )
            self.middleware._channel.queue_bind(
                queue=self.routing_key,
                exchange=settings.CALIBRATION_EXCHANGE,
                routing_key=self.routing_key,
            )
            logging.info(
                f"Successfully bound queue {self.routing_key} to {settings.CALIBRATION_EXCHANGE}"
            )

            logging.info(f"Starting consumption from queue: {self.routing_key}")
            self.middleware.basic_consume(
                queue_name=self.routing_key,
                callback_function=self._handle_message,
            )

            # Start consuming messages
            logging.info(f"Starting message consumption for client {self.client_id}")
            self.middleware.start()

        except Exception as e:
            logging.error(
                f"Error in processing thread for client {self.client_id}: {e}"
            )
        finally:
            with self._lock:
                self._running = False
            logging.info(f"Processing stopped for client {self.client_id}")

    def stop_processing(self):
        """Stop processing and clean up resources."""
        with self._lock:
            if not self._running:
                return

            self._running = False
            logging.info(f"Stopping processing for client {self.client_id}")

        try:
            # Close the middleware connection
            self.middleware.close()

            # End the MLflow run
            self._mlflow_logger.end_run()

        except Exception as e:
            logging.error(f"Error stopping processing for client {self.client_id}: {e}")

    def _handle_message(self, ch, method, properties, body):
        """Handle messages from both dataset and calibration exchanges."""
        logging.info(
            f"Received message for client {self.client_id}, delivery_tag: {method.delivery_tag}"
        )
        try:
            # Try to parse as DataBatch
            try:
                message = dataset_pb2.DataBatch()
                message.ParseFromString(body)
                logging.info(
                    f"Successfully parsed DataBatch for client {self.client_id}, batch_index: {message.batch_index}"
                )
                self._handle_data_message(ch, method, properties, message)
                return
            except Exception as e:
                logging.debug(f"Failed to parse as DataBatch: {e}")
                pass
            # Try to parse as Predictions
            try:
                message = calibration_pb2.Predictions()
                message.ParseFromString(body)
                logging.info(
                    f"Successfully parsed Predictions for client {self.client_id}, batch_index: {message.batch_index}"
                )
                self._handle_probability_message(ch, method, properties, message)
                return
            except Exception as e:
                logging.debug(f"Failed to parse as Predictions: {e}")
                pass
            # If we get here, the message type is unknown
            logging.error(f"Unknown message type received for client {self.client_id}")
            ch.basic_ack(
                delivery_tag=method.delivery_tag
            )  # Acknowledge to remove from queue
        except Exception as e:
            logging.error(f"Error processing message for client {self.client_id}: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _handle_data_message(self, ch, method, properties, message):
        try:
            logging.debug(
                f"Client {self.client_id}: Received data batch {message.batch_index}"
            )
            image_shape = (1, 28, 28)
            image_dtype = np.float32
            image_size = np.prod(image_shape)
            images = np.frombuffer(message.data, dtype=image_dtype)
            num_floats = images.size
            num_images = num_floats // image_size
            if num_images * image_size != num_floats:
                raise ValueError("Incompatible data size for image")
            images = images.reshape((num_images, *image_shape))
            self._store_data(message.batch_index, images, message.is_last_batch)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(
                f"Error handling data message for client {self.client_id}: {e}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _handle_probability_message(self, ch, method, properties, message):
        try:
            logging.debug(
                f"Client {self.client_id}: Received probability batch {message.batch_index}"
            )
            probs = [list(p.values) for p in message.pred]
            probs_array = np.array(probs, dtype=np.float32)
            self._store_probabilities(message.batch_index, probs_array, message.eof)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(
                f"Error handling probability message for client {self.client_id}: {e}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _store_data(self, batch_index: int, inputs: np.ndarray, is_last_batch: bool):
        with self._batches_lock:
            if batch_index not in self._batches:
                self._batches[batch_index] = {
                    "inputs": None,
                    "probs": None,
                    "eof": False,
                }
            self._batches[batch_index]["inputs"] = inputs
            if is_last_batch:
                self._batches[batch_index]["eof"] = True
            self._try_process_batch(batch_index)

    def _store_probabilities(
        self, batch_index: int, probs: np.ndarray, is_last_batch: bool
    ):
        with self._batches_lock:
            if batch_index not in self._batches:
                self._batches[batch_index] = {
                    "inputs": None,
                    "probs": None,
                    "eof": False,
                }
            self._batches[batch_index]["probs"] = probs
            if is_last_batch:
                self._batches[batch_index]["eof"] = True
            self._try_process_batch(batch_index)

    def _try_process_batch(self, batch_index: int):
        batch = self._batches[batch_index]
        if batch["inputs"] is not None and batch["probs"] is not None:
            try:
                self._mlflow_logger.log_single_batch(
                    batch_index=batch_index,
                    probs=batch["probs"],
                    inputs=batch["inputs"],
                )
                logging.info(f"Client {self.client_id}: Processed batch {batch_index}")
                del self._batches[batch_index]
            except Exception as e:
                logging.error(
                    f"Error processing batch {batch_index} for client {self.client_id}: {e}"
                )

    def is_running(self) -> bool:
        with self._lock:
            return self._running
