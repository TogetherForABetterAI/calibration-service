from service import MlflowLogger
from google.protobuf.message import Message
from proto import calibration_pb2, dataset_pb2

class Controller():
    def __init__(self):
        self._service = MlflowLogger()

    def process_outputs(self, ch, method, properties, body):
        print(f"Received probabilistic outputs: {body}")
        message = calibration_pb2.Predictions()
        msg_len = message.ParseFromString(body)
        print(f"Parsed message: {message}")
        # msg_id = 0
        # self._service.store_outputs(msg_id, body)
        
    def process_inputs(self, ch, method, properties, body):
        print(f"Received input data: {body}")
        
        message = dataset_pb2.DataBatch()
        msg_len = message.ParseFromString(body)
        
        print(f"Parsed message: {message}")
        # msg_id = 0
        # self._service.store_input_data(msg_id, body)