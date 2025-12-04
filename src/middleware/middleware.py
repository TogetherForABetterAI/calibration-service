import logging
import time
import pika
import pika.exceptions

from src.lib.config import CONNECTION_EXCHANGE, CONNECTION_QUEUE_NAME

class Middleware:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger("middleware")
        self._consuming = False
        self.consumer_tag = None
        self._is_running = False
        self._on_callback = True
        self._shutdown_received = False
        
        self.connect()

    def connect(self):
        """Establish a connection to RabbitMQ with retries."""
        delay = 5  
        max_delay = 60 # maximum delay of 1 minute
        while not self._shutdown_received:
            try:
                credentials = pika.PlainCredentials(self.config.username, self.config.password)
                self.conn = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.config.host, 
                        port=self.config.port, 
                        credentials=credentials, 
                        heartbeat=5000
                    )
                )
                self.logger.info(
                    f"Connected to RabbitMQ at {self.config.host}:{self.config.port} as {self.config.username}"
                )
                return # suceeded
            except (pika.exceptions.AMQPConnectionError, Exception) as e:
                self.logger.error(f"Failed to connect to RabbitMQ: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay = min(delay * 2, max_delay)  # backoff exponential

    def is_running(self):
        return self._is_running
    
    def setup_connection_queue(self, channel, durable=False):
        queue_name = CONNECTION_QUEUE_NAME
        connection_exchange = CONNECTION_EXCHANGE

        self.declare_exchange(
            channel, connection_exchange, exchange_type="fanout", durable=durable
        )

        # Declare the queue
        self.declare_queue(channel, queue_name, durable=durable)

        # Bind the queue to the exchange
        self.bind_queue(channel, queue_name, connection_exchange, routing_key="")

        self.logger.info(
            f"Queue '{queue_name}' created and bound to exchange '{connection_exchange}'"
        )

    def create_channel(self, prefetch_count=1):
        """Create and return a new channel from the shared connection."""
        # Ensure the connection is alive
        if self.conn is None or self.conn.is_closed:
            self.logger.warning("Connection is closed, reconnecting before creating channel...")
            self.connect()

        try:
            channel = self.conn.channel()
            channel.basic_qos(prefetch_count=prefetch_count)
            return channel
        except Exception as e:
            self.logger.error(f"Failed to create channel: {e}")
            self.connect()
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

    def basic_consume(self, channel, queue_name: str, callback_function, consumer_tag=None) -> str:
        self.logger.info(f"Setting up consumer for queue: {queue_name}")
        self.consumer_tag = channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback_wrapper(callback_function),
            auto_ack=False,
            consumer_tag=consumer_tag
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
        
    def callback_wrapper(self, callback_function):
        def wrapper(ch, method, properties, body):
            if not self._is_running:
                return
            try:
                callback_function(ch, method, properties, body)
            except Exception as e:
                self.logger.error(
                    f"action: rabbitmq_callback | result: fail | error: {e}"
                )
                if ch.is_open:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return wrapper

    def start_consuming(self, channel):
        try:
            self._is_running = True
            if channel and channel.is_open:
                self.logger.info("Starting to consume messages")
                channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.error("Received interrupt signal, stopping consumption")
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPConnectionError) as e:
            self.logger.error(f"Connection lost while consuming: {e}")
            raise e

    def close_channel(self, channel):
        try:
            if channel and channel.is_open:
                channel.close()
                self.logger.info("Stopped consuming messages")
        except Exception as e:
            self.logger.error(f"action: rabbitmq_channel_close | result: fail | error: {e}")
            
    def stop_consuming(self, channel):
        self._is_running = False
        if channel and channel.is_open and self.consumer_tag:
            channel.basic_cancel(self.consumer_tag)
            channel.stop_consuming()
            self.logger.info("Stopped consuming messages")

    def delete_queue(self, channel, queue_name: str):
        try:
            channel.queue_delete(queue=queue_name)
            self.logger.info(f"Queue '{queue_name}' deleted successfully")
        except Exception as e:
            self.logger.error(f"Failed to delete queue '{queue_name}': {e}")

    def close_connection(self):
        try:
            if self.conn is not None and not self.conn.is_closed:
                self.conn.close()
            self.logger.info("RabbitMQ connection closed")
        except Exception as e:
            self.logger.error(f"Failed to close RabbitMQ connection: {e}")
            raise e
        
    def handle_sigterm(self):
        self._shutdown_received = True
