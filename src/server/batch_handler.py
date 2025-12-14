import logging
from typing import Dict, List, Union
import numpy as np
from proto import calibration_pb2, mlflow_probs_pb2, dataset_service_pb2
from src.lib.calibration_stages import CalibrationStage
from src.lib.data_types import DataType
from src.lib.config import MLFLOW_EXCHANGE, MLFLOW_ROUTING_KEY
from src.server.utrace_calculator import UtraceCalculator


class BatchHandler:
    def __init__(
        self,
        user_id: str,
        session_id: str,
        on_eof,
        middleware,
        utrace_calculator,
        database=None,
        inputs_format=None,
    ):
        self.user_id = user_id
        self._inputs_eof = False
        self._outputs_eof = False
        self._batches: Dict[int, Dict] = {}
        self._on_eof = on_eof
        self._db = database
        self._middleware = middleware
        self._channel = self._middleware.create_channel()
        self._session_id = session_id
        self._inputs_format = inputs_format
        self.uq = utrace_calculator

    def _build_state(self):
        inputs = self._db.get_inputs_from_session(self._session_id)
        for input in inputs:
            self._restore_inputs_data(input)
            
        outputs = self._db.get_outputs_from_session(self._session_id)
        for output in outputs:
            self._restore_outputs_data(output)

        if self._inputs_eof and self._outputs_eof:
            self._handle_eof()
         
    def _restore_inputs_data(self, body):
        message = dataset_service_pb2.DataBatchLabeled()
        message.ParseFromString(body)
        images = self._process_input_data(message.data)
        self.store_inputs(
            batch_index=message.batch_index, inputs=images, labels=np.array(list(message.labels)), persist=False)
        
        if message.is_last_batch:
            self._inputs_eof = True
        
    def _restore_outputs_data(self, body):
        message = calibration_pb2.Predictions()
        message.ParseFromString(body)
        probs = np.array([list(p.values) for p in message.pred], dtype=np.float32)
        self.store_outputs(batch_index=message.batch_index, probs=probs, persist=False)
        
        if message.eof:
            self._outputs_eof = True

    def handle_sigterm(self):
        """Stop processing and clean up resources."""
        pass

    def get_calibration_results(self):
        metrics = self.uq.get_calibration_results()
        y_pred = np.array([])
        y_true = np.array([])
        for batch in self._batches.values():
            if batch[DataType.PROBS] is not None and batch[DataType.LABELS] is not None:
                probs = batch[DataType.PROBS]
                labels = batch[DataType.LABELS]
                preds = np.argmax(probs, axis=1)
                y_pred = np.concatenate((y_pred, preds))
                y_true =  np.concatenate((y_true, labels.ravel()))

        metrics["raw_data"]["y_pred"] = np.array(y_pred)
        metrics["raw_data"]["y_true"] = np.array(y_true)
        return metrics



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
                    f"Duplicate probabilities for batch {message.batch_index} from client {self.user_id}"
                )
                return

            self.store_outputs(batch_index=message.batch_index, probs=probs, original_body=body, persist=True)
            # scores = run_calibration_algorithm(probs) 

            if message.eof:
                self._outputs_eof = True

            if self._inputs_eof and self._outputs_eof:
                self._handle_eof()

        except Exception as e:
            logging.error(
                f"Error handling probability message for client {self.user_id}: {e}"
            )
            raise e
        
    def _handle_inputs_message(self, ch, body):
        """Handle inputs messages from the inputs queue."""
        try:
            message = dataset_service_pb2.DataBatchLabeled()
            message.ParseFromString(body)
            images = self._process_input_data(message.data)
            logging.info(
                f"action: receive_inputs | result: success | batch_index: {message.batch_index} | is_last_batch: {message.is_last_batch}"
            )
            
            if message.batch_index in self._batches and self._batches[message.batch_index][DataType.INPUTS] is not None:
                # Duplicate batch index received
                logging.warning(
                    f"Duplicate inputs for batch {message.batch_index} from client {self.user_id}"
                )
                return

            self.store_inputs(batch_index=message.batch_index, inputs=images, labels=np.array(list(message.labels)), original_body=body, persist=True)

            if message.is_last_batch:
                self._inputs_eof = True

            if self._inputs_eof and self._outputs_eof:
                self._handle_eof()
                
        except Exception as e:
            logging.error(
                f"Error handling data message for client {self.user_id}: {e}"
            )
            raise e
        
    def _handle_eof(self):
        self.uq.update_stage(CalibrationStage.FINISHED)
        self._on_eof()  

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
            

    def store_outputs(self, batch_index: int, probs: Union[List[float], np.ndarray], original_body: bytes = None, persist: bool = True):
        self._store_data(batch_index, DataType.PROBS, probs, process_entry=persist)
        if persist:
            self._db.write_outputs(
                session_id=self._session_id,
                outputs=original_body,
                batch_index=batch_index,
            )

    def store_inputs(
        self,
        batch_index: int,
        inputs: np.ndarray,
        labels: np.ndarray,
        original_body: bytes = None,    
        persist: bool = True,
    ):
        self._store_data(batch_index, DataType.INPUTS, inputs, process_entry=persist)
        self._store_data(batch_index, DataType.LABELS, labels, process_entry=persist)
        if persist:
            self._db.write_inputs(
                session_id=self._session_id,
                inputs=original_body,
                batch_index=batch_index,
            )


    def _store_data(
        self, batch_index: int, kind: DataType, data: np.ndarray, process_entry: bool = True
    ):
        if batch_index not in self._batches:
            self._batches[batch_index] = {
                DataType.INPUTS: None,
                DataType.PROBS: None,
                DataType.LABELS: None,
            }

        self._batches[batch_index][kind] = data
        entry = self._batches[batch_index]

        if process_entry and all(
            entry[kind] is not None
            for kind in [DataType.INPUTS, DataType.PROBS, DataType.LABELS]
        ):
            self.uq.process_entry(entry)
            self.send_mlflow_msg(batch_index, entry)
            # del self._batches[batch_index]

    def send_mlflow_msg(self, batch_index, entry):
        mlflow_msg = mlflow_probs_pb2.MlflowProbs()
        for p in entry[DataType.PROBS]:
            prob = mlflow_probs_pb2.PredictionList()
            prob.values.extend(p)
            mlflow_msg.pred.append(prob)

        mlflow_msg.batch_index = batch_index
        mlflow_msg.client_id = self.user_id
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
        logging.info(
            f"action: send_mlflow_message | "
            f"user_id: {self.user_id} | "
            f"session_id: {self._session_id} | "
            f"batch_index: {batch_index} | "
            f"result: success"
        )
