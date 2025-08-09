import logging
import time
from typing import Dict, List, Optional
from graphene import Enum
import numpy as np
from google.protobuf.message import Message
from proto import calibration_pb2, dataset_pb2
from service.mlflow_logger import MlflowLogger
from middleware.middleware import Middleware
from service.report_builder import ReportBuilder
from enum import Enum


class DataType(Enum):
    INPUTS = 0
    PROBS = 1
     

class ClientProcessor:
    def __init__(
        self,
        client_id: str,
        queue_calibration: str,
        queue_inter_connection: str,
        middleware: Middleware,
    ):
        self.client_id = client_id
        self.queue_calibration = queue_calibration
        self.queue_inter_connection = queue_inter_connection
        self.middleware = middleware

        self._report_builder = ReportBuilder(client_id=client_id)
        self._eof = False
        self._report_emited = False
        self._batches: Dict[int, Dict] = {}

        # MLflow logger for this client
        self._mlflow_logger = MlflowLogger(client_id=client_id)

        logging.info(f"Initialized ClientProcessor for client {client_id}")

    def start_processing(self):
        """Start processing messages from both calibration and inter-connection queues."""
        logging.info(f"Starting processing for client {self.client_id}")

        try:
            self.middleware.basic_consume(
                queue_name=self.queue_inter_connection,
                callback_function=self._handle_data_message,
            )
            self.middleware.basic_consume(
                queue_name=self.queue_calibration,
                callback_function=self._handle_probability_message,
            )

            logging.info(f"Starting message consumption for client {self.client_id}")
            self.middleware.start()

        except Exception as e:
            logging.error(
                f"Error in client {self.client_id}: {e}"
            )
        finally:
            self._running = False
            logging.info(f"Processing stopped for client {self.client_id}")

    def stop_processing(self):
        """Stop processing and clean up resources."""

        self._mlflow_logger.end_run()
        


    def _handle_data_message(self, ch, method, properties, body):
        """Handle data messages from the inter-connection queue."""
        # try:
        message = dataset_pb2.DataBatch()
        message.ParseFromString(body)
        logging.info(
            f"Successfully parsed DataBatch for client {self.client_id}, batch_index: {message.batch_index}"
        )

        images = self._process_input_data(message.data)

        self.store_input_data(message.batch_index, images, message.is_last_batch)

        if self._eof and not self._report_emited:
            logging.info(f"Entra a eof de input")
            # Get y_pred & y_test from calibration
            self._report_builder.build_report(None, None)
            self.stop_processing()
            self._report_builder.send_report()
            self._report_emited = True
                
        # except Exception as e:
        #     logging.error(
        #         f"Error handling data message for client {self.client_id}: {e}"
        #     )
        #     raise e 

    def _process_input_data(self, data):
        image_shape = (1, 28, 28)
        image_dtype = np.float32
        image_size = np.prod(image_shape)
        images = np.frombuffer(data, dtype=image_dtype)
        num_floats = images.size
        num_images = num_floats // image_size
        if num_images * image_size != num_floats:
            raise ValueError("Incompatible data size for image")
        
        return images.reshape((num_images, *image_shape))

    def _handle_probability_message(self, ch, method, properties, body):
        """Handle probability messages from the calibration queue."""

        message = calibration_pb2.Predictions()
        message.ParseFromString(body)
        logging.info(
            f"Successfully parsed Predictions for client {self.client_id}, batch_index: {message.batch_index}"
        )

        probs = [list(p.values) for p in message.pred]
        # labels = [p.label for p in message.pred]
        
        probs_array = np.array(probs, dtype=np.float32)
        self.store_outputs(message.batch_index, probs_array, message.eof)
        
        if self._eof and not self._report_emited:
            logging.info(f"Entra a eof de probs")
            # Get y_pred & y_test from calibration
            self._report_builder.build_report(None, None)
            self.stop_processing()
            self._report_builder.send_report()
            self._report_emited = True

        # except Exception as e:
        #     logging.error(
        #         f"Error handling probability message for client {self.client_id}: {e}"
        #     )
        #     raise e

    def store_outputs(self, batch_index: int, probs: np.ndarray, is_last_batch: bool):
        self._store_data(batch_index, DataType.PROBS, probs, is_last_batch)

    def store_input_data(
        self, batch_index: int, inputs: np.ndarray, is_last_batch: bool
    ):
        self._store_data(batch_index, DataType.INPUTS, inputs, is_last_batch)
                
    def _store_data(self, batch_index: int, kind: DataType, data: np.ndarray, eof: bool):
        if batch_index not in self._batches:
            self._batches[batch_index] = {DataType.INPUTS: None, DataType.PROBS: None}

        self._batches[batch_index][kind] = data
        entry = self._batches[batch_index]
        
        if entry[DataType.INPUTS] is not None and entry[DataType.PROBS] is not None:
            self._mlflow_logger.log_single_batch(batch_index, entry[DataType.PROBS], entry[DataType.INPUTS])
            del self._batches[batch_index]
            if eof:
                self._eof = True

            
