from unittest.mock import Mock


class FakeMiddleware:
    def __init__(self, config, messages: dict=None):
        self.conn = Mock()
        self.conn.is_open = True
        self.conn.close.return_value = None
        self.logger = Mock()
        self.messages = messages
        self.callbacks = {}
        self.stop_consunming_called = False
        self.close_connection_called = False
        self.close_channel_called = False
        self.config = config

    def create_channel(self, prefetch_count=1):
        self.channel = Mock()
        self.channel.is_open = True
        return self.channel

    def setup_connection_queue(self, channel, durable: bool = False):
        pass

    def declare_queue(self, channel, queue_name: str, durable: bool = False):
        pass


    def declare_exchange(
        self,
        channel,
        exchange_name: str,
        exchange_type: str = "fanout",
        durable: bool = False,
    ):
        pass

    def bind_queue(
        self,
        channel,
        queue_name: str,
        exchange_name: str,
        routing_key: str = "",
    ):
        pass

    def basic_consume(self, channel, queue_name: str, on_message_callback):
        self.callbacks[queue_name] = on_message_callback

    def start_consuming(self, channel):
        for cb_queue_name, callback in self.callbacks.items():
            for msg_queue_name in self.messages:
                if msg_queue_name in cb_queue_name:
                    for msg in self.messages[msg_queue_name]:
                        message = msg.encode("utf-8")
                        callback(
                            channel,
                            Mock(),  # method
                            Mock(),  # properties
                            message,  # body
                        )

        

    def unsuscribe_from_queue(self, channel, consumer_tag):
        pass

    def close_channel(self, channel):
        self.close_channel_called = True

    def close_connection(self):
        self.close_connection_called = True

    def callback_wrapper(self, callback_function):
        def wrapper(ch, method, properties, body):
            callback_function(ch, method, properties, body)
        return wrapper
    
    def stop_consuming(self, channel):
        self.stop_consunming_called = True