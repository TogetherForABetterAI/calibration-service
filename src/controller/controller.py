from service import MlflowLogger
from google.protobuf.message import Message

class Controller():
    def __init__(self):
        self._service = MlflowLogger()

    def process_outputs(self, ch, method, properties, body):
        print(f"Received probabilistic outputs: {body}")
        message = Message.ParseFromString(body)
        print(f"Parsed message: {message}")
        # msg_id = 0
        # self._service.store_outputs(msg_id, body)
        
    def process_inputs(self, ch, method, properties, body):
        print(f"Received input data: {body}")
        message = Message.ParseFromString(body)
        print(f"Parsed message: {message}")
        # msg_id = 0
        # self._service.store_input_data(msg_id, body)