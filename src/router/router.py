from controller import Controller
from lib.config import PROB_OUTPUTS_QUEUE_NAME, DATA_INPUTS_QUEUE_NAME

class Router():
    def __init__(self, middleware):
        self._middleware = middleware
        self._controller = Controller()
        
    def start(self):
        self._middleware.basic_consume(queue_name=PROB_OUTPUTS_QUEUE_NAME, callback_function=self.outputs_callback)
        self._middleware.basic_consume(queue_name=DATA_INPUTS_QUEUE_NAME, callback_function=self.inputs_callback)
        self._middleware.start()
        
    def outputs_callback(self, ch, method, properties, body):
        self._controller.process_outputs(body)
        
    def inputs_callback(self, ch, method, properties, body):
        self._controller.process_inputs(body)