import pika
import logging
from config.settings import settings


class Middleware:
    def __init__(self):
        # Create connection parameters using settings
        credentials = pika.PlainCredentials(
            settings.RABBITMQ_USER, settings.RABBITMQ_PASS
        )
        parameters = pika.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

        # Establish connection
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()

        # Configure channel for reliable delivery
        self._channel.confirm_delivery()
        self._channel.basic_qos(prefetch_count=1)  # Fair load distribution

        logging.info(
            f"Connected to RabbitMQ at {settings.RABBITMQ_HOST}:{settings.RABBITMQ_PORT}"
        )

    def callback_wrapper(self, callback_function):
        def wrapper(ch, method, properties, body):
            try:
                callback_function(ch, method, properties, body)
                # Manual acknowledgment after successful processing
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logging.error(f"action: rabbitmq_callback | result: fail | error: {e}")
                # Reject and requeue on error
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        return wrapper

    def basic_send(
        self, routing_key: str = "", message: str = "", exchange_name: str = ""
    ):
        try:
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )

        except Exception as e:
            logging.error(
                f"action: message_sending | result: fail | error: {e} | routing_key: {routing_key}"
            )

    def basic_consume(self, queue_name: str, callback_function):
        """
        Start consuming from a queue.

        Note: Queues must be pre-declared by the authenticator service.
        This method only consumes from existing queues, it does not declare them.
        """
        # Use auto_ack=False for manual acknowledgment
        self._channel.basic_consume(
            queue=queue_name,
            on_message_callback=self.callback_wrapper(callback_function),
            auto_ack=False,  # Manual acknowledgment
        )

    def start(self):
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            logging.info("Received interrupt signal, stopping consumption")
            self.close()
        except Exception as e:
            logging.error(f"Error during consumption: {e}")
            self.close()

    def close(self):
        try:
            if hasattr(self, "_channel") and self._channel:
                self._channel.close()
            if hasattr(self, "_connection") and self._connection:
                self._connection.close()
            logging.info("RabbitMQ connection closed")
        except Exception as e:
            logging.error(
                f"action: rabbitmq_connection_close | result: fail | error: {e}"
            )

    def is_connected(self):
        """Check if the connection is still active."""
        try:
            return self._connection and not self._connection.is_closed
        except:
            return False
