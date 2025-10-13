import threading
import logging
import queue


class ShutdownMonitor(threading.Thread):
    """Monitors the shutdown queue and signals shutdown to the main thread"""

    def __init__(self, shutdown_queue, shutdown_callback):
        super().__init__(name="QueryRepliesHandler-ShutdownMonitor", daemon=False)
        self.shutdown_queue = shutdown_queue
        self.shutdown_callback = shutdown_callback
        self.logger = logging.getLogger(__name__)

    def run(self):
        """Monitor shutdown queue for shutdown signals"""
        try:
            self.logger.info(
                "action: shutdown_monitor_start | result: success | msg: shutdown monitor started"
            )

            # Block until shutdown signal is received
            shutdown_signal = self.shutdown_queue.get(block=True)

            if shutdown_signal is None:  # Shutdown signal
                self.logger.info(
                    "action: shutdown_signal_received | result: success | msg: shutdown signal received"
                )

                # Call the shutdown callback to notify main thread
                if self.shutdown_callback:
                    self.shutdown_callback()

        except Exception as e:
            self.logger.error(
                "action: shutdown_monitor | result: fail | msg: error in shutdown monitor | error: %s",
                e,
            )
