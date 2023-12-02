import os
import time
from redis import Redis
import json
from dotenv import load_dotenv
import pika
from pika.exchange_type import ExchangeType
from async_consumer import Consumer
import logging
import asyncio

load_dotenv()

LOG_FORMAT = ('[%(asctime)s] %(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT,
                    datefmt="%Y-%m-%d %H:%M:%S")


# REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://localhost:5672')


# redis = Redis.from_url(REDIS_URL, decode_responses=True)
# credentials = pika.credentials.PlainCredentials(
#     'esbot', 'esbot', erase_on_connect=True)
# rabbit = pika.SelectConnection(
#     parameters=pika.ConnectionParameters('localhost', credentials=credentials))


def parse(message):

    try:
        return json.loads(message)
    except Exception as e:

        if isinstance(message, bytes):
            return message.decode('utf-8')
        message


def consumer():

    channel_name = 'channel_name'
    print(f'Waiting for messages on channel: "{channel_name}"...')
    while True:
        _, message = redis.blpop(channel_name, timeout=0)
        if message:
            message = parse(message)
            print(message)


def subscriber():

    channel_name = 'report_tokens'
    print(f'Waiting for messages on channel: "{channel_name}"...')
    pubsub = redis.pubsub()
    pubsub.subscribe(channel_name)
    for message in pubsub.listen():
        if message['type'] == 'message':
            message = parse(message['data'])
            print(message)


def process_message(message):
    print(message)


def streams():

    redis.xgroup_createconsumer(
        name='test_stream',
        groupname='test_group',
        consumername='worker-1',

    )
    print('Waiting for messages on stream: "test_stream"...')
    while True:

        messages = redis.xreadgroup(
            groupname='test_group',
            consumername='worker-1',
            streams={'test_stream': '>'},
            count=1,
            block=0,
        )

        if messages:
            for message in messages[0][1]:
                process_message(message)

        else:
            time.sleep(1)


def rabbit_on_message_received(channel, method, properties, body):
    print(body)
    if (method.delivery_tag % 2 == 0):
        channel.basic_ack(delivery_tag=method.delivery_tag, multiple=False)
    else:
        channel.basic_nack(delivery_tag=method.delivery_tag, multiple=False)
    time.sleep(0.5)


async def rabbit_consumer():

    exchange_name = 'test'
    exchange_name_2 = 'test2'
    queue_name = 'test-queue'
    queue_name_2 = 'test-queue2'
    routing_key = 'test'
    routing_key_2 = 'test_2'
    consumer = Consumer(amqp_url=RABBITMQ_URL, exchange_name=exchange_name,
                        queue_name=queue_name, routing_key=routing_key)
    consumer_2 = Consumer(amqp_url=RABBITMQ_URL, exchange_name=exchange_name_2,
                          queue_name=queue_name_2, routing_key=routing_key_2)

    await asyncio.gather(consumer.run(), consumer_2.run())
    # t = asyncio.gather(asyncio.to_thread(consumer.run()), asyncio.to_thread(
    #     consumer_2.run()))
    # channel = rabbit.channel()
    # channel.exchange_declare(
    #     exchange=exchange_name, exchange_type=ExchangeType.fanout)
    # channel.queue_declare(queue=queue_name)
    # channel.queue_bind(exchange=exchange_name,
    #                    queue=queue_name, routing_key=routing_key)

    # channel.basic_consume(
    #     queue=queue_name,
    #     on_message_callback=rabbit_on_message_received,
    # )
    # print('Waiting for messages on queue: "test-queue"...')

    # rabbit.ioloop.start()
    # channel.start_consuming()

    counter = 0
    while True:
        counter += 1
        print(f'waiting for - {counter} seconds')
        time.sleep(1)


if __name__ == '__main__':
    try:
        # consumer()
        # subscriber()
        # streams()
        # time.sleep(5)
        asyncio.run(rabbit_consumer())
        # rabbit_consumer()
    except KeyboardInterrupt:
        print('\nexiting')
        exit(0)
