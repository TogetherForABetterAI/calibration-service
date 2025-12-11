
from src.server.client_manager import ClientManager
import pika


class ClientManagerHandler:
    def __init__(self, process_handler: ClientManager, ch: pika.channel.Channel, delivery_tag: int):
        self.process_handler = process_handler
        self.ch = ch
        self.delivery_tag = delivery_tag

    def send_nack(self):
        """Send a negative acknowledgment for the client's message."""
        self.ch.basic_nack(delivery_tag=self.delivery_tag, requeue=True)

    def send_ack(self):
        """Send an acknowledgment for the client's message."""
        self.ch.basic_ack(delivery_tag=self.delivery_tag)

    def terminate(self):
        """Terminate the client manager process."""
        self.process_handler.terminate()

    def join(self):
        """Join the client manager process."""
        self.process_handler.join()

    def is_alive(self) -> bool:
        """Check if the client manager process is alive."""
        return self.process_handler.is_alive()