import logging
from lib.config import DATASET_EXCHANGE, REPLIES_EXCHANGE


class Consumer:
    """
    Consumer handles consuming from multiple queues using a shared connection
    but with its own dedicated channel.
    """

    def __init__(
        self,
        middleware,  # object with host, port, credentials, etc.
        client_id,
        labeled_callback,
        replies_callback,
        logger=None,
    ):
        self.middleware = middleware
        self.channel = self.middleware.create_channel(prefetch_count=1)  # new channel
        self.client_id = client_id
        self.logger = logger or logging.getLogger(f"consumer-{client_id}")
        self.labeled_queue_name = f"{client_id}_labeled_queue"
        self.replies_queue_name = f"{client_id}_replies_queue"
        self.routing_key_labeled = f"{client_id}.labeled"
        self.routing_key_replies = f"{client_id}"
        self.labeled_exchange = DATASET_EXCHANGE
        self.replies_exchange = REPLIES_EXCHANGE
        self.labeled_callback = labeled_callback  # Callback for labeled queue
        self.replies_callback = replies_callback  # Callback for replies queue
        self._shutdown_initiated = False

    def start(self):
        logging.info(f"Starting Consumer for client {self.client_id}")
        """Declare/bind queues, start consuming, and ACK the original message."""
        try:
            self._setup_queues()
            if not self._shutdown_initiated:
                self.middleware.start_consuming(self.channel)
                logging.info(f"Consumer started for client {self.client_id}")
        except Exception as e:
            self.logger.error(f"Error in Consumer for client {self.client_id}: {e}")
        finally:
            self.graceful_finish()

    def _labeled_callback(self, ch, method, properties, body):
        """Wrapper callback for labeled queue messages."""
        self.labeled_callback(ch, method, properties, body)

    def _replies_callback(self, ch, method, properties, body):
        """Wrapper callback for replies queue messages."""
        self.replies_callback(ch, method, properties, body)

    def handle_sigterm(self):
        self._shutdown_initiated = True
        if self.middleware.is_running():
            self.middleware.stop_consuming()
        
        if not self.middleware.on_callback():
            self.middleware.cancel_channel_consuming(self.channel)

    def graceful_finish(self):
        """Gracefully shutdown the consumer."""
        self.middleware.delete_queue(channel=self.channel, queue_name=self.labeled_queue_name)
        self.middleware.delete_queue(channel=self.channel, queue_name=self.replies_queue_name)
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()
        self.logger.info(f"Consumer shutdown complete for client {self.client_id}")

    def _setup_queues(self):
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
