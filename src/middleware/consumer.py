import logging
from src.lib.config import INPUTS_QUEUE_NAME, OUTPUTS_QUEUE_NAME


class Consumer:
    """
    Consumer handles consuming from multiple queues using a shared connection
    but with its own dedicated channel.
    """

    def __init__(
        self,
        middleware,  # object with host, port, credentials, etc.
        user_id,
        inputs_callback=None,
        predictions_callback=None,
        logger=None,
    ):
        self.middleware = middleware
        self.channel = self.middleware.create_channel(prefetch_count=1)  # new channel
        self.user_id = user_id
        self.logger = logger or logging.getLogger(f"consumer-{user_id}")
        self.inputs_queue_name = f"{user_id}_{INPUTS_QUEUE_NAME}"
        self.outputs_queue_name = f"{user_id}_{OUTPUTS_QUEUE_NAME}"
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
            self.logger.error(f"Error in Consumer for client {self.user_id}: {e}")
        finally:
            self.finish()

    def _inputs_callback(self, ch, method, properties, body):
        """Wrapper callback for inputs queue messages."""
        if self.inputs_callback:
            self.inputs_callback(ch, method, properties, body)

    def _predictions_callback(self, ch, method, properties, body):
        """Wrapper callback for predictions queue messages."""
        if self.predictions_callback:
            self.predictions_callback(ch, method, properties, body)

    def handle_sigterm(self):
        self._shutdown_initiated = True
        self.middleware.stop_consuming()

    def finish(self):
        """Gracefully shutdown the consumer."""
        self.middleware.close_channel(self.channel)
        self.middleware.close_connection()
        self.logger.info(f"Consumer shutdown complete for client {self.user_id}")

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
