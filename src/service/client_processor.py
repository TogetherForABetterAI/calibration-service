import logging
from typing import Dict, List
import numpy as np
from proto import calibration_pb2, dataset_pb2
from service.mlflow_logger import MlflowLogger
from middleware.middleware import Middleware
from service.report_builder import ReportBuilder
from lib.data_types import DataType

class ClientProcessor:
    def __init__(
        self,
        client_id: str,
        outputs_queue_calibration: str,
        inputs_queue_calibration: str,
        middleware: Middleware,
    ):
        self.client_id = client_id
        self.outputs_queue_calibration = outputs_queue_calibration
        self.inputs_queue_calibration = inputs_queue_calibration
        self.middleware = middleware

        self._report_builder = ReportBuilder(client_id=client_id)
        self._eof = False
        self._batches: Dict[int, Dict] = {}

        # MLflow logger for this client
        self._mlflow_logger = MlflowLogger(client_id=client_id)

        logging.info(f"Initialized ClientProcessor for client {client_id}")

    def start_processing(self):
        """Start processing messages from both calibration and inter-connection queues."""
        logging.info(f"Starting processing for client {self.client_id}")

        # try:
        self.middleware.basic_consume(
            queue_name=self.inputs_queue_calibration,
            callback_function=self._handle_data_message,
        )
        self.middleware.basic_consume(
            queue_name=self.outputs_queue_calibration,
            callback_function=self._handle_probability_message,
        )

        logging.info(f"Starting message consumption for client {self.client_id}")
        self.middleware.start()

        # except Exception as e:
        #     logging.error(
        #         f"Error in client {self.client_id}: {e}"
        #     )
        # finally:
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
            f"action: receive_data_batch | result: success | eof {message.is_last_batch}"
        )
 
        images = self._process_input_data(message.data)

        self.store_input_data(message.batch_index, images, message.is_last_batch, list(message.labels))

        if self._eof:
            #TODO: Get y_pred & y_test from calibration
            y_pred = [probs for batch in self._batches.values() for probs in batch[DataType.PROBS]]
            y_test = [labels for batch in self._batches.values() for labels in batch[DataType.LABELS]]
            self._report_builder.build_report(y_test, y_pred)
            logging.info(f"action: build_report | result: success")
            self.stop_processing()
            self._report_builder.send_report()
            logging.info(f"action: send_report | result: success")
                
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

        logging.info(f"MSG LEN: {len(body)}")
        message = calibration_pb2.Predictions()
        message.ParseFromString(body)

        logging.info(
            f"action: receive_predictions | result: success | eof {message.eof}"
        )

        probs = [list(p.values) for p in message.pred]
        
        probs_array = np.array(probs, dtype=np.float32)
        self.store_outputs(message.batch_index, probs_array, message.eof)
        if self._eof:
            #TODO: Get y_pred & y_test from calibration
            y_pred = [probs for batch in self._batches.values() for probs in batch[DataType.PROBS]]
            y_test = [labels for batch in self._batches.values() for labels in batch[DataType.LABELS]]
            self._report_builder.build_report(y_test, y_pred)
            logging.info(f"action: build_report | result: success")
            self.stop_processing()
            self._report_builder.send_report()
            logging.info(f"action: send_report | result: success")

        # except Exception as e:
        #     logging.error(
        #         f"Error handling probability message for client {self.client_id}: {e}"
        #     )
        #     raise e

    def store_outputs(self, batch_index: int, probs: np.ndarray, is_last_batch: bool):
        self._store_data(batch_index, DataType.PROBS, probs, is_last_batch)


    def store_input_data(
        self, batch_index: int, inputs: np.ndarray, is_last_batch: bool, labels: np.ndarray
    ):
        self._store_data(batch_index, DataType.INPUTS, inputs, is_last_batch)
        self._store_data(batch_index, DataType.LABELS, labels, is_last_batch)
                
    def _store_data(self, batch_index: int, kind: DataType, data: np.ndarray, eof: bool):
        if batch_index not in self._batches:
            self._batches[batch_index] = {DataType.INPUTS: None, DataType.PROBS: None, DataType.LABELS: None}

        self._batches[batch_index][kind] = data
        entry = self._batches[batch_index]

        # Enhanced: Only log and delete batch if all required data is present
        if all(entry[kind] is not None for kind in [DataType.INPUTS, DataType.PROBS, DataType.LABELS]):
            self._mlflow_logger.log_single_batch(
            batch_index,
            entry[DataType.PROBS],
            entry[DataType.INPUTS],
            entry[DataType.LABELS]
            )
            # del self._batches[batch_index] # Lo comento por ahora, porque uso estos datos para armar el reporte (provisoriamente hasta tener acceso al paquete UQM), pero es correcto que se borren.
            
            if eof:
                self._eof = True


            
