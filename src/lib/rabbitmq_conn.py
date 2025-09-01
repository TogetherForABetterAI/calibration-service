import pika
from lib.config import config_params

def connect_to_rabbitmq():
    return pika.BlockingConnection(pika.ConnectionParameters(config_params["rabbitmq_host"], config_params["rabbitmq_port"]))
