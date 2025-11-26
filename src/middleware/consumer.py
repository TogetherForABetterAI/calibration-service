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
        self.inputs_queue_name = f"{client_id}_inputs_queue"
        self.outputs_queue_name = f"{client_id}_outputs_queue"
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
        self.middleware.declare_queue(
            self.channel, self.inputs_queue_name, durable=True
        )

        self.middleware.basic_consume(
            self.channel, self.inputs_queue_name, self._inputs_callback
        )

        self.middleware.basic_consume(
            self.channel, self.outputs_queue_name, self._predictions_callback
        )
