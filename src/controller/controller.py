from service import MlflowLogger

class Controller():
    def __init__(self):
        self._service = MlflowLogger()

    def process_outputs(self, ch, method, properties, body):
        print(f"Received probabilistic outputs: {body}")
        msg_id = 0
        self._service.store_outputs(msg_id, body)
        
    def process_inputs(self, ch, method, properties, body):
        print(f"Received input data: {body}")
        msg_id = 0
        self._service.store_input_data(msg_id, body)