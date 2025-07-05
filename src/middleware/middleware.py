import pika
import os 
import logging

class Middleware:
    def __init__(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
        channel = connection.channel()
        channel.confirm_delivery()
        channel.basic_qos(prefetch_count=1)
        self._channel = channel
        
    def declare_queue(self, queue_name):
        self._channel.queue_declare(queue=queue_name, durable=True)
    
    def declare_exchange(self, exchange_name, exchange_type='direct'):
        self._channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        
    def bind_queue(self, queue_name, exchange_name, routing_key=""):
        self._channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)

    def callback_wrapper(self, callback_function):
        def wrapper(ch, method, properties, body):
            try:
                callback_function(ch, method, properties, body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(
                    f"action: rabbitmq_callback | result: fail | error: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
        return wrapper
        
    def basic_send(self, routing_key: str = "", message: str = "", exchange_name: str = ""):
        try:
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
            )
    
        except Exception as e:
            logging.error(
                f"action: message_sending | result: fail | error: {e} | routing_key: {routing_key}")
            
    def basic_consume(self, queue_name: str, callback_function):
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback_wrapper(callback_function)
        )
    
    def start(self):
        try:
            self._channel.start_consuming()
        except OSError:
            self.close()
        
    def close(self):
        try:
            self._channel.close()
            self._connection.close()
        except Exception as e:
            logging.error(
                f"action: rabbitmq_connection_close | result: fail | error: {e}")
    
