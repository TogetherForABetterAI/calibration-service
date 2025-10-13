import logging
import threading
from lib.constants import DATASET_EXCHANGE, REPLIES_EXCHANGE
import pika


class Consumer(threading.Thread):
    """
    Consumer handles consuming from multiple queues using a shared connection
    but with its own dedicated channel. Now runs as a Thread.
    """

    def __init__(
        self,
        middleware_config,  # object with host, port, credentials, etc.
        client_id,
        labeled_callback=None,
        replies_callback=None,
        logger=None,
    ):
        super().__init__(name=f"Consumer-{client_id}", daemon=False)
        self.middleware_config = middleware_config
        self.client_id = client_id
        self.connection = None  # Will be created in run()
        self.channel = None  # Will be created in run()
        self.logger = logger or logging.getLogger(f"consumer-{client_id}")
        self.labeled_queue_name = f"{client_id}_labeled_queue"
        self.replies_queue_name = f"{client_id}_replies_queue"
        self.routing_key_labeled = f"{client_id}.labeled"
        self.routing_key_replies = client_id
        self.labeled_callback = labeled_callback  # Callback for labeled queue
        self.replies_callback = replies_callback  # Callback for replies queue

    def run(self):
        """Declare/bind queues, start consuming, and ACK the original message."""
        self.logger.info(f"Consumer thread started for client {self.client_id}")

        try:
            # Create a new connection for this thread
            self.logger.info(
                f"[STEP] Creating RabbitMQ connection for client {self.client_id}"
            )
            params = {
                "host": self.middleware_config.host,
                "port": self.middleware_config.port,
                "heartbeat": getattr(self.middleware_config, "heartbeat", 5000),
            }
            if hasattr(self.middleware_config, "username") and hasattr(
                self.middleware_config, "password"
            ):
                params["credentials"] = pika.PlainCredentials(
                    self.middleware_config.username, self.middleware_config.password
                )
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(**params)
            )
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)

            # Declare and bind queues
            self.logger.info(f"[STEP] Declaring exchange for client {self.client_id}")
            self.channel.exchange_declare(
                exchange=REPLIES_EXCHANGE, exchange_type="direct", durable=False
            )

            self.channel.exchange_declare(
                exchange=DATASET_EXCHANGE, exchange_type="direct", durable=False
            )

            self.logger.info(
                f"[STEP] Declaring and binding queues for client {self.client_id}"
            )
            self.channel.queue_declare(queue=self.labeled_queue_name, durable=False)

            self.channel.queue_bind(
                queue=self.labeled_queue_name,
                exchange=DATASET_EXCHANGE,
                routing_key=self.routing_key_labeled,
            )

            self.channel.queue_declare(queue=self.replies_queue_name, durable=False)

            self.channel.queue_bind(
                queue=self.replies_queue_name,
                exchange=REPLIES_EXCHANGE,
                routing_key=self.routing_key_replies,
            )

            self.logger.info(
                f"[STEP] Queues declared and bound for client {self.client_id}"
            )

            # Start consuming from both queues
            self.channel.basic_consume(
                queue=self.labeled_queue_name,
                on_message_callback=self._labeled_callback,
                auto_ack=False,
            )

            self.channel.basic_consume(
                queue=self.replies_queue_name,
                on_message_callback=self._replies_callback,
                auto_ack=False,
            )

            self.logger.info(f"Starting consumption for client {self.client_id}")
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(
                f"Error in Consumer thread for client {self.client_id}: {e}"
            )

    def _labeled_callback(self, ch, method, properties, body):
        """Wrapper callback for labeled queue messages."""
        try:
            if self.labeled_callback:
                self.labeled_callback(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.debug(
                f"ACK labeled message with delivery_tag={method.delivery_tag}"
            )
        except Exception as e:
            self.logger.error(
                f"Error in labeled callback for client {self.client_id}: {e}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.logger.warning(
                f"NACK labeled message with delivery_tag={method.delivery_tag}, requeuing"
            )

    def _replies_callback(self, ch, method, properties, body):
        """Wrapper callback for replies queue messages."""
        try:
            if self.replies_callback:
                self.replies_callback(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.logger.debug(
                f"ACK replies message with delivery_tag={method.delivery_tag}"
            )
        except Exception as e:
            self.logger.error(
                f"Error in replies callback for client {self.client_id}: {e}"
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            self.logger.warning(
                f"NACK replies message with delivery_tag={method.delivery_tag}, requeuing"
            )

    def shutdown(self):
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
            self.logger.info(
                f"Consumer stopped and channel closed for client {self.client_id}"
            )
        except Exception as e:
            self.logger.error(
                f"Error stopping consumer for client {self.client_id}: {e}"
            )
