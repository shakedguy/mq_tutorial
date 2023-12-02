import functools
import logging
import threading
import time
import pika
from pika.exchange_type import ExchangeType
from typing import Callable, Any, List

LOG_FORMAT = ('[%(asctime)s] %(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING, format=LOG_FORMAT,
                    datefmt="%Y-%m-%d %H:%M:%S")


class ThreadedConsumer(object):

    def __init__(self, url, queue_name: str, exchange_name: str, routing_key: str, prefetch_count: int = 1):
        self.url: str = url
        self.queue_name: str = queue_name
        self.exchange_name: str = exchange_name
        self.routing_key: str = routing_key
        self.callback: Callable[..., Any] | None = None
        self.prefetch_count: int = prefetch_count
        self.threads: List[threading.Thread] = []
        self.connection = None
        self.channel = None
        self.on_message_callback = None
        self.parameters = None
        self.credentials = None

    def __del__(self):
        self.connection.close()

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        logger.info('Connecting to %s', self.url)
        return pika.BlockingConnection(parameters=pika.URLParameters(self.url))

    def ack_message(self, ch, delivery_tag):
        """Note that `ch` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if ch.is_open:
            ch.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

    def do_work(self, ch, delivery_tag, body):
        thread_id = threading.get_ident()
        print('Thread id: %s Delivery tag: %s Message body: %s', thread_id,
              delivery_tag, body)
        # Sleeping to simulate 10 seconds of work
        time.sleep(10)
        cb = functools.partial(self.ack_message, ch, delivery_tag)
        ch.connection.add_callback_threadsafe(cb)

    def on_message(self, ch, method_frame, _header_frame, body, args):
        thrds = args
        delivery_tag = method_frame.delivery_tag
        t = threading.Thread(target=self.do_work,
                             args=(ch, delivery_tag, body))
        t.start()
        thrds.append(t)

    def run(self, callback: Callable[..., Any]):

        self.connection = self.connect()
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type=ExchangeType.fanout)
        self.channel.queue_declare(queue=self.queue_name)
        self.channel.queue_bind(exchange=self.exchange_name,
                                queue=self.queue_name, routing_key='test')

        self.on_message_callback = functools.partial(
            callback, args=(self.threads))
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message_callback,
        )
        print('Waiting for messages on queue: "test-queue"...')

        try:
            t = threading.Thread(target=self.channel.start_consuming)
            t.start()
            return t
        except Exception as e:
            logger.error(e)
            self.channel.stop_consuming()
