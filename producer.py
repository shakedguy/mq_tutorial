from redis import Redis, exceptions
import time
import dotenv
import os
import json
import pika
from pika.exchange_type import ExchangeType
from bullmq import Queue
import asyncio

dotenv.load_dotenv()

REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')


queue = Queue(name="test", redisOpts={
              'host': 'localhost', 'port': 6379, 'db': 0})


class Message(object):
    def __init__(self, action: str, number: int):
        self.action = action
        self.number = number


async def add_job():
    await queue.add("test", {"action": "start", "number": 20})


asyncio.get_event_loop().run_until_complete(add_job())
# asyncio.get_event_loop().run_until_complete(add_job())


# redis = Redis.from_url(REDIS_URL, decode_responses=True)
# credentials = pika.credentials.PlainCredentials(
#     'esbot', 'esbot', erase_on_connect=True)
# rabbit = pika.BlockingConnection(
#     pika.ConnectionParameters('localhost', credentials=credentials))


# def producer():

#     i = 1
#     channel_name = 'report_tokens'
#     print(f'Start producing messages on channel: "{channel_name}"...')
#     while True:
#         message = f'Message Number {i}'
#         redis.rpush(channel_name, message)
#         print(f'Produced Message Number {i}')
#         i += 1


# def publisher():

#     i = 1
#     channel_name = 'report_tokens'
#     print(f'Start publishing messages on channel: "{channel_name}"...')
#     message = {'action': 'start', 'number': 20}
#     message = {'action': 'start', 'number': 20}

#     while True:
#         message = f'Message Number {i}'
#         redis.publish(channel_name, message)
#         print(f'Publish Message Number {i}')
#         i += 1
#         time.sleep(1)


# def create_group(key: str, groupname: str, id: str, mkstream: bool = False):

#     try:
#         redis.xgroup_create(
#             name=key,
#             groupname=groupname,
#             id=id,
#             mkstream=mkstream
#         )
#     except Exception as e:
#         print(e)


# def streams():

#     create_group(
#         key='test_stream',
#         groupname='test_group',
#         id='$',
#         mkstream=False
#     )
#     redis.xadd(
#         name='test_stream',
#         fields={
#             'action': 'start',
#             'number': 20,
#         },
#     )
#     redis.xautoclaim(
#         name='test_stream',
#         groupname='test_group',
#         min_idle_time=1000,
#         consumername='worker-2',

#     )


# def rabbit_producer():

#     exchange_name = 'test'
#     exchange_name_2 = 'test2'
#     queue_name = 'test-queue'
#     queue_name_2 = 'test-queue2'
#     routing_key = 'test'
#     routing_key_2 = 'test_2'
#     channel = rabbit.channel()
#     channel.exchange_declare(
#         exchange=exchange_name, exchange_type=ExchangeType.fanout)
#     channel.queue_declare(queue=queue_name)
#     counter = 0
#     msg = f"Message Number {counter}"
#     while counter < 10:
#         counter += 1
#         msg = f"Message Number {counter}"
#         channel.basic_publish(exchange=exchange_name,
#                               routing_key=routing_key, body=msg)
#         time.sleep(0.5)


# if __name__ == '__main__':

#     try:
#         # producer()
#         # publisher()
#         # streams()
#         redis.lpush("bull:test:stalled-check", b"test")
#         # rabbit_producer()
#     except KeyboardInterrupt:
#         print('\nexiting')
#     redis.close()
#     # rabbit.close()
