from middleware import Middleware
from lib.config import PROB_OUTPUTS_QUEUE_NAME, DATA_INPUTS_QUEUE_NAME, EXCHANGE_NAME, EXCHANGE_TYPE

def setup_rabbitmq():
    middleware = Middleware()
    middleware.declare_exchange(exchange_name=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)
    middleware.declare_queue(queue_name=PROB_OUTPUTS_QUEUE_NAME)
    middleware.declare_queue(queue_name=DATA_INPUTS_QUEUE_NAME)
    middleware.bind_queue(queue_name=PROB_OUTPUTS_QUEUE_NAME, exchange_name=EXCHANGE_NAME, routing_key="probs")
    middleware.bind_queue(queue_name=DATA_INPUTS_QUEUE_NAME, exchange_name=EXCHANGE_NAME, routing_key="data")
    return middleware
    