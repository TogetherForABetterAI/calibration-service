import logging
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
        self.start_consuming_called = False
        self.msg_ack = []
        self.msg_nack = []
        self._is_running = False
        self.stop_consuming_called = False
        self.messages_sent = []

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

    def run(self):
        self.start_consuming_called = True
        
    def basic_consume(self, channel, queue_name: str, callback_function):
        self.callbacks[queue_name] = callback_function

    def start_consuming(self, channel):
        self.start_consuming_called = True
        self._is_running = True
        for cb_queue_name, callback in self.callbacks.items():
            for msg_queue_name in self.messages:
                if msg_queue_name in cb_queue_name:
                    for msg in self.messages[msg_queue_name]:
                        try: 
                            callback(
                                channel,
                                Mock(),  # method
                                Mock(),  # properties
                                msg,  # body
                            )
                            self.msg_ack.append(msg)
                        except Exception as e:
                            self.msg_nack.append(msg)

        logging.info("FakeMiddleware finishing consumption...")
        self._is_running = False

    def unsuscribe_from_queue(self, channel, consumer_tag):
        pass

    def basic_send(
        self, 
        channel,
        exchange_name: str,
        routing_key: str,
        message_body: str,
    ):
        self.messages_sent.append((exchange_name, routing_key, message_body))
        
    def close_channel(self, channel):
        self.close_channel_called = True

    def close_connection(self):
        self.close_connection_called = True

    def callback_wrapper(self, callback_function):
        def wrapper(ch, method, properties, body):
            callback_function(ch, method, properties, body)
        return wrapper
    
    def stop_consuming(self):
        self.stop_consuming_called = True
        self._is_running = False

    def delete_queue(self, channel, queue_name: str):   
        pass

    def is_running(self):
        return self._is_running