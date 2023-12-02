import os
import time
from redis import Redis
import json
from dotenv import load_dotenv


load_dotenv()


REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
WORKER_ID = 'worker-2'

redis = Redis.from_url(REDIS_URL, decode_responses=True)


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
        consumername=WORKER_ID,
    )

    print('Waiting for messages on stream: "test_stream"...')
    while True:

        messages = redis.xpending_range(
            name='test_stream',
            groupname='test_group',
            min='-',
            max='+',
            count=1,
            consumername=WORKER_ID,
        )
        if messages:
            for message in messages:
                print(message)
                process_message(message)
                redis.xack(
                    'test_stream',
                    'test_group',
                    message['message_id'],

                )

        else:
            time.sleep(1)


if __name__ == '__main__':
    try:
        # consumer()
        # subscriber()
        streams()
    except KeyboardInterrupt:
        print('\nexiting')
        exit(0)
