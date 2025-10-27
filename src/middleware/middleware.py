import pika
import logging

from src.lib.config import CONNECTION_EXCHANGE, CONNECTION_QUEUE_NAME, COORDINATOR_EXCHANGE


class Middleware:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("middleware")
        self._consuming = False
        self.consumer_tag = None
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

    def setup_connection_queue(self, channel, durable=False):
        queue_name = CONNECTION_QUEUE_NAME
        connection_exchange = CONNECTION_EXCHANGE
        coordinator_exchange = COORDINATOR_EXCHANGE # Provisorio. Hasta que este el microservicio y hagamos la creacion desde ahi.

        # Declare the exchange (fanout type for broadcasting)
        self.declare_exchange(
            channel, connection_exchange, exchange_type="fanout", durable=durable
        )
        self.declare_exchange(
            channel, coordinator_exchange, exchange_type="fanout", durable=durable
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
        message_body: str,
        properties: pika.BasicProperties = None,
    ):
        try:
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message_body,
                properties=properties,
            )
        except Exception as e:
            self.logger.error(
                f"Failed to send message to exchange '{exchange_name}' "
                f"with routing key '{routing_key}': {e}"
            )
            raise e 
        
    def callback_wrapper(self, callback_function):
        def wrapper(ch, method, properties, body):
            try:
                callback_function(ch, method, properties, body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                self.logger.error(
                    f"action: rabbitmq_callback | result: fail | error: {e}"
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        return wrapper

    def start_consuming(self, channel):
        try:
            if channel and channel.is_open:
                self.logger.info("Starting to consume messages")
                channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.error("Received interrupt signal, stopping consumption")
        
            
    def stop_consuming(self, channel):
        try:
            if channel and channel.is_open:
                channel.basic_cancel(self.consumer_tag)
                channel.stop_consuming()
                channel.close()
                self.logger.info("Stopped consuming messages")
        except Exception as e:
            self.logger.error(f"Failed to stop consuming messages: {e}")
            raise e
        
    def close_connection(self, channel):
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
                self.logger.info("RabbitMQ connection closed successfully")
        except Exception as e:
            self.logger.error(f"Failed to close RabbitMQ connection: {e}")
            raise e
        