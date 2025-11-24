import logging
from typing import Dict, List, Union
import numpy as np
from proto import calibration_pb2, mlflow_probs_pb2, dataset_service_pb2
from src.lib.data_types import DataType
from src.lib.config import MLFLOW_EXCHANGE, MLFLOW_ROUTING_KEY
from src.lib.report_builder import ReportBuilder


class BatchHandler:
    def __init__(
        self,
        client_id: str,
        session_id: str,
        on_eof,
        report_builder,
        middleware,
        database=None,
        inputs_format=None,
    ):
        self.client_id = client_id
        self._report_builder = report_builder
        self._labeled_eof = False
        self._replies_eof = False
        self._batches: Dict[int, Dict] = {}
        self._on_eof = on_eof
        self._db = database
        self._middleware = middleware
        self._channel = self._middleware.create_channel()
        self._scores = self._db.get_scores_from_session(client_id)
        self._session_id = session_id
        self._inputs_format = inputs_format
        self._build_state()
        """
        The line above should change to store only scores instead of probabilities per class and labels.
        self._batches: Dict[int, np.ndarray] = {}
        """

    def _build_state(self):
        inputs = self._db.get_inputs_from_session(self._session_id)
        if len(inputs) > 0:
            for input in inputs:
                self._restore_inputs_data(input)
            
        outputs = self._db.get_outputs_from_session(self._session_id)
        if len(outputs) > 0:
            for output in outputs:
                self._handle_predictions_message(None, output)
         
    def _restore_inputs_data(self, body):
        message = dataset_service_pb2.DataBatchLabeled()
        message.ParseFromString(body)
        images = self._process_input_data(message.data)
        self.store_inputs(
            message.batch_index, images, message.is_last_batch, list(message.labels)
        )
        
    def _restore_outputs_data(self, body):
        message = calibration_pb2.Predictions()
        message.ParseFromString(body)
        probs = np.array([list(p.values) for p in message.pred], dtype=np.float32)
        self.store_outputs(message.batch_index, probs)
            
    def handle_sigterm(self):
        """Stop processing and clean up resources."""
        pass

    def send_report(self):
        """
        Build and send report when both labeled and replies data are complete.
        y_pred and y_test are the outputs of the calibration process. For now, we build them from stored data.
        In the future, we should change this to use the scores only.

        y_pred: List of predicted probabilities
        y_test: List of true labels
        """
        y_pred = []
        y_test = []
        for batch in self._batches.values():
            if batch[DataType.PROBS] is not None and batch[DataType.LABELS] is not None:
                y_pred.extend(batch[DataType.PROBS])
                y_test.extend(batch[DataType.LABELS])

        self._report_builder.build_report(y_test, y_pred)
        logging.info(f"action: build_report | result: success")
        #self._report_builder.send_report("guldenjf@gmail.com")
        logging.info(f"action: send_report | result: success")


    def _handle_predictions_message(self, ch, body):
        """Handle probability messages from the calibration queue."""

        try:
            message = calibration_pb2.Predictions()
            message.ParseFromString(body)

            logging.info(
                f"action: receive_predictions | result: success | eof {message.eof}"
            )

            probs = np.array([list(pred_list.values) for pred_list in message.pred], dtype=np.float32)

            if message.batch_index in self._batches and self._batches[message.batch_index][DataType.PROBS] is not None:
                # Duplicate batch index received
                logging.warning(
                    f"Duplicate probabilities for batch {message.batch_index} from client {self.client_id}"
                )
                return

            self.store_outputs(message.batch_index, probs)
            # scores = run_calibration_algorithm(probs) 

            if message.eof:
                self.send_report()
                self._on_eof() 

        except Exception as e:
            logging.error(
                f"Error handling probability message for client {self.client_id}: {e}"
            )
            raise e
        
    def _handle_inputs_message(self, ch, body):
        """Handle inputs messages from the inputs queue."""
        try:
            message = dataset_service_pb2.DataBatchLabeled()
            message.ParseFromString(body)
            images = self._process_input_data(message.data)
            
            if message.batch_index in self._batches and self._batches[message.batch_index][DataType.INPUTS] is not None:
                # Duplicate batch index received
                logging.warning(
                    f"Duplicate inputs for batch {message.batch_index} from client {self.client_id}"
                )
                return

            self.store_inputs(message.batch_index, images, list(message.labels))

            if message.is_last_batch:
                self._labeled_eof = True

            if self._labeled_eof and self._replies_eof:
                self._send_report()

            if self._on_eof and self._labeled_eof and self._replies_eof:
                self._on_eof()  # Received both EOFs, time to finish consuming
        except Exception as e:
            logging.error(
                f"Error handling data message for client {self.client_id}: {e}"
            )
            raise e
        
    def _process_input_data(self, data):
        
        data_array = np.frombuffer(data, dtype=self._inputs_format.dtype)
        
        data_size = np.prod(self._inputs_format.shape)
        num_elements = data_array.size
        num_samples = num_elements // data_size

        if num_samples * data_size != num_elements:
            raise ValueError(
                f"Data size incompatible with expected format. "
                f"Expected elements per sample: {data_size}, "
                f"total elements: {num_elements}, "
                f"calculated samples: {num_samples}, "
                f"remainder: {num_elements % data_size}"
            )

        try:
            data_array = data_array.reshape((num_samples, *self._inputs_format.shape))
        except Exception as e:
            raise ValueError(f"Error reshaping data: {e}")
        
        if len(data_array.shape) == 4:
            H, W = data_array.shape[1], data_array.shape[2]
            C = data_array.shape[3] if data_array.shape[-1] in [1, 3] else None

            # detect HWC only if last dim is channels
            if data_array.shape[-1] in [1, 3] and H != 1:
                data_array = np.transpose(data_array, (0, 3, 1, 2))

        return data_array
            

    def store_outputs(self, batch_index: int, probs: Union[List[float], np.ndarray]):
        self._store_data(batch_index, DataType.PROBS, probs)

    def store_inputs(
        self,
        batch_index: int,
        inputs: np.ndarray,
        labels: np.ndarray,
    ):
        self._store_data(batch_index, DataType.INPUTS, inputs)
        self._store_data(batch_index, DataType.LABELS, labels)

    def _store_data(
        self, batch_index: int, kind: DataType, data: np.ndarray
    ):
        if batch_index not in self._batches:
            self._batches[batch_index] = {
                DataType.INPUTS: None,
                DataType.PROBS: None,
                DataType.LABELS: None,
            }

        self._batches[batch_index][kind] = data
        entry = self._batches[batch_index]

        if all(
            entry[kind] is not None
            for kind in [DataType.INPUTS, DataType.PROBS, DataType.LABELS]
        ):
            mlflow_msg = mlflow_probs_pb2.MlflowProbs()
            for p in entry[DataType.PROBS]:
                prob = mlflow_probs_pb2.PredictionList()
                prob.values.extend(p)
                mlflow_msg.pred.append(prob)

            mlflow_msg.batch_index = batch_index
            mlflow_msg.client_id = self.client_id
            mlflow_msg.session_id = self._session_id
            mlflow_msg.data = entry[DataType.INPUTS].tobytes()
            mlflow_msg.labels.extend(entry[DataType.LABELS].tolist())
            mlflow_body = mlflow_msg.SerializeToString()

            self._middleware.basic_send(
                channel=self._channel,  
                exchange_name=MLFLOW_EXCHANGE,
                routing_key=MLFLOW_ROUTING_KEY,
                body=mlflow_body,
            )
