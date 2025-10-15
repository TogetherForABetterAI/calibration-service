import logging
import threading
import time
from lib.constants import DATASET_EXCHANGE, REPLIES_EXCHANGE
from middleware.middleware import Middleware


class Consumer:
    """
    Consumer handles consuming from multiple queues using a shared connection
    but with its own dedicated channel.
    """

    def __init__(
        self,
        middleware_config,  # object with host, port, credentials, etc.
        client_id,
        labeled_callback=None,
        replies_callback=None,
        logger=None,
    ):
        self.middleware = Middleware(middleware_config)  # create new connection
        self.client_id = client_id
        self.logger = logger or logging.getLogger(f"consumer-{client_id}")
        self.labeled_queue_name = f"{client_id}_labeled_queue"
        self.replies_queue_name = f"{client_id}_replies_queue"
        self.routing_key_labeled = f"{client_id}.labeled"
        self.routing_key_replies = f"{client_id}"
        self.acknowledged = False
        self.labeled_exchange = DATASET_EXCHANGE
        self.replies_exchange = REPLIES_EXCHANGE
        self.labeled_callback = labeled_callback  # Callback for labeled queue
        self.replies_callback = replies_callback  # Callback for replies queue
        self.channel = None
        self.shutdown_initiated = False
        self._stop_consuming_called = False
        self.shutdown_lock = threading.Lock()

    def start(self):
        """Declare/bind queues, start consuming, and ACK the original message."""
        self.logger.info(f"Consumer thread started for client {self.client_id}")
        try:

            self.channel = self.middleware.create_channel(prefetch_count=1)

            # Declare and bind labeled queue to DATASET_EXCHANGE
            self.middleware.declare_exchange(
                self.channel,
                self.labeled_exchange,
                exchange_type="direct",
                durable=False,
            )

            self.middleware.declare_queue(
                self.channel, self.labeled_queue_name, durable=False
            )

            self.middleware.bind_queue(
                self.channel,
                self.labeled_queue_name,
                self.labeled_exchange,
                self.routing_key_labeled,
            )

            # Declare and bind replies queue to REPLIES_EXCHANGE
            self.middleware.declare_exchange(
                self.channel,
                self.replies_exchange,
                exchange_type="direct",
                durable=False,
            )

            self.middleware.declare_queue(
                self.channel, self.replies_queue_name, durable=False
            )

            self.middleware.bind_queue(
                self.channel,
                self.replies_queue_name,
                self.replies_exchange,
                self.routing_key_replies,
            )

            self.middleware.basic_consume(
                self.channel, self.labeled_queue_name, self._labeled_callback
            )

            self.middleware.basic_consume(
                self.channel, self.replies_queue_name, self._replies_callback
            )

            threading.Thread(target=self._check_flag).start()  # express thread

            self.middleware.start_consuming(self.channel)
        except Exception as e:
            self.logger.error(
                f"Error in Consumer thread for client {self.client_id}: {e}"
            )
        finally:
            if self.channel and self.channel.is_open:
                self.middleware.close_channel(self.channel)
                self.middleware.close_connection()
                self.logger.info(
                    f"Channel and connection closed for client {self.client_id}"
                )

    def _labeled_callback(self, ch, method, properties, body):
        """Wrapper callback for labeled queue messages."""
        # Note: ACK/NACK is handled by middleware's callback_wrapper
        try:
            if self.labeled_callback:
                self.labeled_callback(ch, method, properties, body)
        except Exception as e:
            self.logger.error(
                f"Error in labeled callback for client {self.client_id}: {e}"
            )
            raise

    def _replies_callback(self, ch, method, properties, body):
        """Wrapper callback for replies queue messages."""
        # Note: ACK/NACK is handled by middleware's callback_wrapper
        try:
            if self.replies_callback:
                self.replies_callback(ch, method, properties, body)
        except Exception as e:
            self.logger.error(
                f"Error in replies callback for client {self.client_id}: {e}"
            )
            raise

    def stop_consuming(self):
        try:
            if self.channel and self.channel.is_open:
                self.middleware.stop_consuming(self.channel)
                self.logger.info(
                    f"Consumer stopped and channel closed for client {self.client_id}"
                )

        except Exception as e:
            self.logger.error(
                f"Error stopping consumer for client {self.client_id}: {e}"
            )

    # In case "stop_consuming" is called before "start_consuming"
    # (due to shutdown signal), we check the flag after "start_consuming" is called.
    # This way we ensure that if shutdown was initiated while "start_consuming"
    # was being called, we still stop consuming.
    # If we don't do this, the process will never exit
    # because "start_consuming" is blocking.
    # So after "start_consuming" is called, we wait and check the flag.
    def _check_flag(self):
        time.sleep(2)
        with self.shutdown_lock:
            shutdown = self.shutdown_initiated
        if shutdown:
            self.stop_consuming()

    def set_shutdown(self):
        with self.shutdown_lock:
            self.shutdown_initiated = True
