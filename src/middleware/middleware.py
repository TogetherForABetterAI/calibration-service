import pika
import logging

from src.lib.config import CONNECTION_EXCHANGE, CONNECTION_QUEUE_NAME


class Middleware:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("middleware")
        self._consuming = False
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
        exchange_name = CONNECTION_EXCHANGE

        # Declare the exchange (fanout type for broadcasting)
        self.declare_exchange(
            channel, exchange_name, exchange_type="fanout", durable=durable
        )

        # Declare the queue
        self.declare_queue(channel, queue_name, durable=durable)

        # Bind the queue to the exchange
        self.bind_queue(channel, queue_name, exchange_name, routing_key="")

        self.logger.info(
            f"Queue '{queue_name}' created and bound to exchange '{exchange_name}'"
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

    def basic_consume(self, channel, queue_name: str, callback_function):
        self.logger.info(f"Setting up consumer for queue: {queue_name}")
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback_wrapper(callback_function),
            auto_ack=False,
        )

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
            self.logger.info("Received interrupt signal, stopping consumption")

    def stop_consuming(self, channel):
        if channel and channel.is_open:
            channel.stop_consuming()
            self.logger.info("Stopped consuming messages")

    def close_channel(self, channel):
        try:
            if channel and channel.is_open:
                channel.close()
                self.logger.info("RabbitMQ channel closed")
        except Exception as e:
            self.logger.error(
                f"action: rabbitmq_connection_close | result: fail | error: {e}"
            )

    def close_connection(self):
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
                self.logger.info("RabbitMQ connection closed")
        except Exception as e:
            self.logger.error(
                f"action: rabbitmq_connection_close | result: fail | error: {e}"
            )
