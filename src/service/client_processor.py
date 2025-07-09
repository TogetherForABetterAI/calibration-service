import threading
import logging
import time
from typing import Dict, Optional
import numpy as np
from google.protobuf.message import Message
from proto import calibration_pb2, dataset_pb2
from service.mlflow_logger import MlflowLogger
from middleware.middleware import Middleware


class ClientProcessor:
    def __init__(
        self,
        client_id: str,
        inter_queue_name: str,
        client_queue_name: str,
        middleware: Middleware,
    ):
        self.client_id = client_id
        self.inter_queue_name = inter_queue_name
        self.client_queue_name = client_queue_name
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
        """Start processing messages from both queues."""
        with self._lock:
            if self._running:
                logging.warning(
                    f"ClientProcessor for {self.client_id} is already running"
                )
                return

            self._running = True
            logging.info(f"Starting processing for client {self.client_id}")

        try:
            # Set up consumers for both queues
            self.middleware.basic_consume(
                queue_name=self.inter_queue_name,
                callback_function=self._handle_data_message,
            )

            self.middleware.basic_consume(
                queue_name=self.client_queue_name,
                callback_function=self._handle_probability_message,
            )

            # Start consuming messages
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

    def _handle_data_message(self, ch, method, properties, body):
        """Handle data messages from the inter_queue (data-dispatcher-service)."""
        try:
            message = dataset_pb2.DataBatch()
            message.ParseFromString(body)

            logging.debug(
                f"Client {self.client_id}: Received data batch {message.batch_index}"
            )

            # Parse the image data
            image_shape = (1, 28, 28)
            image_dtype = np.float32
            image_size = np.prod(image_shape)

            images = np.frombuffer(message.data, dtype=image_dtype)
            num_floats = images.size
            num_images = num_floats // image_size

            if num_images * image_size != num_floats:
                raise ValueError("Incompatible data size for image")

            images = images.reshape((num_images, *image_shape))

            # Store the data and check if we can process this batch
            self._store_data(message.batch_index, images, message.is_last_batch)

            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(
                f"Error processing data message for client {self.client_id}: {e}"
            )
            # Reject the message and requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _handle_probability_message(self, ch, method, properties, body):
        """Handle probability messages from the client_queue (SDK)."""
        try:
            message = calibration_pb2.Predictions()
            message.ParseFromString(body)

            logging.debug(
                f"Client {self.client_id}: Received probability batch {message.batch_index}"
            )

            # Parse the probabilities
            probs = [list(p.values) for p in message.pred]
            probs_array = np.array(probs, dtype=np.float32)

            # Store the probabilities and check if we can process this batch
            self._store_probabilities(message.batch_index, probs_array, message.eof)

            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(
                f"Error processing probability message for client {self.client_id}: {e}"
            )
            # Reject the message and requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _store_data(self, batch_index: int, inputs: np.ndarray, is_last_batch: bool):
        """Store input data for a batch."""
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

            # Check if we can process this batch
            self._try_process_batch(batch_index)

    def _store_probabilities(
        self, batch_index: int, probs: np.ndarray, is_last_batch: bool
    ):
        """Store probabilities for a batch."""
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

            # Check if we can process this batch
            self._try_process_batch(batch_index)

    def _try_process_batch(self, batch_index: int):
        """Try to process a batch if both inputs and probabilities are available."""
        batch = self._batches[batch_index]

        if batch["inputs"] is not None and batch["probs"] is not None:
            try:
                # Process the batch with MLflow logging
                self._mlflow_logger.log_single_batch(
                    batch_index=batch_index,
                    probs=batch["probs"],
                    inputs=batch["inputs"],
                )

                logging.info(f"Client {self.client_id}: Processed batch {batch_index}")

                # Remove the processed batch
                del self._batches[batch_index]

            except Exception as e:
                logging.error(
                    f"Error processing batch {batch_index} for client {self.client_id}: {e}"
                )

    def is_running(self) -> bool:
        """Check if the processor is currently running."""
        with self._lock:
            return self._running
