import logging
import pika

from src.lib.config import CONNECTION_EXCHANGE, CONNECTION_QUEUE_NAME, COORDINATOR_EXCHANGE

class Middleware:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("middleware")
        self._consuming = False
        self.consumer_tag = None
        self._is_running = False
        self._on_callback = True
        try:
            self.conn = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=config.host, port=config.port, heartbeat=5000
                )
            )
            self.logger.info(
                f"Connected to RabbitMQ at {config.host}:{config.port} as {config.username}"
            )
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise e

    def is_running(self):
        return self._is_running
    
    def setup_connection_queue(self, channel, durable=False):
        queue_name = CONNECTION_QUEUE_NAME
        connection_exchange = CONNECTION_EXCHANGE
        coordinator_exchange = COORDINATOR_EXCHANGE # Provisorio. Hasta que este el microservicio y hagamos la creacion desde ahi.

        self.declare_exchange(
            channel, connection_exchange, exchange_type="fanout", durable=durable
        )
        self.declare_exchange(
            channel, coordinator_exchange, exchange_type="direct", durable=durable
        )

        # Declare the queue
        self.declare_queue(channel, queue_name, durable=durable)

        # Bind the queue to the exchange
        self.bind_queue(channel, queue_name, connection_exchange, routing_key="")

        self.logger.info(
            f"Queue '{queue_name}' created and bound to exchange '{connection_exchange}'"
        )

    def create_channel(self, prefetch_count=1):
        """Create and return a new channel from the shared connection, with optional prefetch_count."""
        channel = self.conn.channel()
        channel.basic_qos(prefetch_count=prefetch_count)
        return channel

    def declare_queue(self, channel, queue_name: str, durable: bool = False):
        try:
            channel.queue_declare(queue=queue_name, durable=durable)
            self.logger.info(f"Queue '{queue_name}' declared successfully")
        except Exception as e:
            self.logger.error(f"Failed to declare queue '{queue_name}': {e}")
            raise e

    def declare_exchange(
        self,
        channel,
        exchange_name: str,
        exchange_type: str = "direct",
        durable: bool = False,
    ):
        try:
            channel.exchange_declare(
                exchange=exchange_name, exchange_type=exchange_type, durable=durable
            )
            self.logger.info(f"Exchange '{exchange_name}' declared successfully")
        except Exception as e:
            self.logger.error(f"Failed to declare exchange '{exchange_name}': {e}")
            raise e

    def bind_queue(
        self, channel, queue_name: str, exchange_name: str, routing_key: str
    ):
        try:
            channel.queue_bind(
                exchange=exchange_name, queue=queue_name, routing_key=routing_key
            )
            self.logger.info(
                f"Queue '{queue_name}' bound to exchange '{exchange_name}' "
                f"with routing key '{routing_key}'"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to bind queue '{queue_name}' to exchange '{exchange_name}' "
                f"with routing key '{routing_key}': {e}"
            )
            raise e

    def basic_consume(self, channel, queue_name: str, callback_function) -> str:
        self.logger.info(f"Setting up consumer for queue: {queue_name}")
        self.consumer_tag = channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback_wrapper(callback_function),
            auto_ack=False,
        )
    
    def basic_send(
        self, 
        channel,
        exchange_name: str,
        routing_key: str,
        body: bytes,
        properties: pika.BasicProperties = None,
    ):
        try:
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=body,
                properties=properties,
            )
        except Exception as e:
            self.logger.error(
                f"Failed to send message to exchange '{exchange_name}' "
                f"with routing key '{routing_key}': {e}"
            )
            raise e 
    
    def on_callback(self):
        return self._on_callback
        
    def callback_wrapper(self, callback_function):
        self._on_callback = True
        def wrapper(ch, method, properties, body):
            if not self._is_running:
                self.cancel_channel_consuming(ch)
                return
            
            try:
                callback_function(ch, method, properties, body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                self.logger.error(
                    f"action: rabbitmq_callback | result: fail | error: {e}"
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            if not self._is_running:
                self.cancel_channel_consuming(ch)

        self._on_callback = False
        return wrapper

    def start_consuming(self, channel):
        try:
            self._is_running = True
            if channel and channel.is_open:
                self.logger.info("Starting to consume messages")
                channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.error("Received interrupt signal, stopping consumption")

    def close_channel(self, channel):
        try:
            if self._is_running:
                raise Exception("Cannot close channel while middleware is running")
            
            if channel and channel.is_open:
                channel.basic_cancel(self.consumer_tag)
                channel.stop_consuming()
                channel.close()
                self.logger.info("Stopped consuming messages")
        except Exception as e:
            self.logger.error(f"action: rabbitmq_channel_close | result: fail | error: {e}")

    def cancel_channel_consuming(self, channel):
        if channel and channel.is_open:
            self.logger.info(f"Cancelling consumer for channel: {channel}")
            channel.basic_cancel(consumer_tag=self.consumer_tag)
            channel.stop_consuming()
            
    def stop_consuming(self):
        self._is_running = False
        self.logger.info("Stopped consuming messages")

    def delete_queue(self, channel, queue_name: str):
        try:
            channel.queue_delete(queue=queue_name)
            self.logger.info(f"Queue '{queue_name}' deleted successfully")
        except Exception as e:
            self.logger.error(f"Failed to delete queue '{queue_name}': {e}")

    def close_connection(self):
        try:
            if self._is_running:
                raise Exception("Cannot close connection while middleware is running")
            
            self.conn.close()
            self.logger.info("RabbitMQ connection closed")
        except Exception as e:
            self.logger.error(f"Failed to close RabbitMQ connection: {e}")
            raise e
        