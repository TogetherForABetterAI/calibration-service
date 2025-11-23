import logging
from lib.config import INPUTS_EXCHANGE, REPLIES_EXCHANGE


class Consumer:
    """
    Consumer handles consuming from multiple queues using a shared connection
    but with its own dedicated channel.
    """

    def __init__(
        self,
        middleware,  # object with host, port, credentials, etc.
        client_id,
        inputs_callback=None,
        predictions_callback=None,
        logger=None,
    ):
        self.middleware = middleware
        self.channel = self.middleware.create_channel(prefetch_count=1)  # new channel
        self.client_id = client_id
        self.logger = logger or logging.getLogger(f"consumer-{client_id}")
        self.labeled_queue_name = f"{client_id}_labeled_queue"
        self.replies_queue_name = f"{client_id}_calibration_queue"
        # self.routing_key_labeled = f"{client_id}.labeled"
        # self.routing_key_replies = f"{client_id}"
        # self.inputs_exchange = INPUTS_EXCHANGE
        # self.replies_exchange = REPLIES_EXCHANGE
        self.inputs_callback = inputs_callback  # Callback for inputs queue
        self.predictions_callback = predictions_callback  # Callback for replies queue
        self._shutdown_initiated = False

    def start(self):
        """Declare/bind queues, start consuming, and ACK the original message."""
        try:
            self._setup_queues()
            if not self._shutdown_initiated:
                self.middleware.start_consuming(self.channel)
        except Exception as e:
            self.logger.error(f"Error in Consumer for client {self.client_id}: {e}")
        finally:
            self.shutdown()

    def _inputs_callback(self, ch, method, properties, body):
        """Wrapper callback for inputs queue messages."""
        if self._shutdown_initiated:
            self.middleware.stop_consuming(self.channel)
            # Note: ACK/NACK is handled by middleware's callback_wrapper

        if self.inputs_callback:
            self.inputs_callback(ch, method, properties, body)

    def _predictions_callback(self, ch, method, properties, body):
        """Wrapper callback for predictions queue messages."""
        if self._shutdown_initiated:
            self.middleware.stop_consuming(self.channel)
        # Note: ACK/NACK is handled by middleware's callback_wrapper

        if self.predictions_callback:
            self.predictions_callback(ch, method, properties, body)

    def stop_consuming(self):
        try:
            self.middleware.stop_consuming(self.channel)
            self.logger.info(
                f"Consumer stopped and channel closed for client {self.client_id}"
            )
        except Exception as e:
            self.logger.error(
                f"Error stopping consumer for client {self.client_id}: {e}"
            )

    def handle_sigterm(self):
        self._shutdown_initiated = True

    def shutdown(self):
        """Gracefully shutdown the consumer."""
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()
        self.logger.info(f"Consumer shutdown complete for client {self.client_id}")

    def _setup_queues(self):
        # Declare and bind labeled queue to INPUTS_EXCHANGE
        # self.middleware.declare_exchange(
        #     self.channel,
        #     self.inputs_exchange,
        #     exchange_type="direct",
        #     durable=False,
        # )

        self.middleware.declare_queue(
            self.channel, self.labeled_queue_name, durable=False
        )

        # self.middleware.bind_queue(
        #     self.channel,
        #     self.labeled_queue_name,
        #     self.inputs_exchange,
        #     self.routing_key_labeled,
        # )

        # Declare and bind replies queue to REPLIES_EXCHANGE
        # self.middleware.declare_exchange(
        #     self.channel,
        #     self.replies_exchange,
        #     exchange_type="direct",
        #     durable=False,
        # )

        # self.middleware.declare_queue(
        #     self.channel, self.replies_queue_name, durable=False
        # )

        # self.middleware.bind_queue(
        #     self.channel,
        #     self.replies_queue_name,
        #     self.replies_exchange,
        #     self.routing_key_replies,
        # )

        self.middleware.basic_consume(
            self.channel, self.labeled_queue_name, self._inputs_callback
        )

        self.middleware.basic_consume(
            self.channel, self.replies_queue_name, self._predictions_callback
        )
