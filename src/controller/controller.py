from service import MlflowLogger
from google.protobuf.message import Message
from proto import calibration_pb2, dataset_pb2
import numpy as np
import logging

class Controller():
    def __init__(self):
        self._service = MlflowLogger()

    def process_outputs(self, ch, method, properties, body):
        message = calibration_pb2.Predictions()
        message.ParseFromString(body)

        # logging.info(f"Parsed_OUTPUTS: {message}")
        logging.info(f"[OUTPUTS] Batch index: {message.batch_index}, is_last_batch: {message.eof}")

        probs = [list(p.values) for p in message.pred]
        probs_array = np.array(probs, dtype=np.float32)

        self._service.store_outputs(
            batch_index=message.batch_index,
            probs=probs_array,
            is_last_batch=message.eof
        )

        
    def process_inputs(self, ch, method, properties, body):    
        message = dataset_pb2.DataBatch()
        message.ParseFromString(body)

        #logging.info(f"Labels: {message.labels}")
        logging.info(f"[INPUTS] Batch index: {message.batch_index}, is_last_batch: {message.is_last_batch}")

        image_shape = (1, 28, 28)
        image_dtype = np.float32
        image_size = np.prod(image_shape)

        images = np.frombuffer(message.data, dtype=image_dtype)
        num_floats = images.size
        num_images = num_floats // image_size

        if num_images * image_size != num_floats:
            raise ValueError("Tamaño de datos incompatible con imagen")

        images = images.reshape((num_images, *image_shape))
        labels = np.array(message.labels, dtype=np.int32)

        self._service.store_input_data(
            batch_index=message.batch_index,
            inputs=images,
            is_last_batch=message.is_last_batch,
            labels=labels  
        )
