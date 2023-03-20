from os import getenv
from redis import Redis
from time import sleep
import json
from dotenv import load_dotenv


load_dotenv()


REDIS_PORT = getenv('REDIS_PORT', 6379)
REDIS_DB = getenv('REDIS_DB', 0)
REDIS_HOST = getenv('REDIS_HOST', 'localhost')
REDIS_PASSWORD = getenv('REDIS_PASSWORD', None)

mq = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
)


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
        _, message = mq.blpop(channel_name, timeout=0)
        if message:
            message = parse(message)
            print(message)


def subscriber():

    channel_name = 'report_tokens'
    print(f'Waiting for messages on channel: "{channel_name}"...')
    pubsub = mq.pubsub()
    pubsub.subscribe(channel_name)
    for message in pubsub.listen():
        if message['type'] == 'message':
            message = parse(message['data'])
            print(message)


if __name__ == '__main__':
    try:
        # consumer()
        subscriber()
    except KeyboardInterrupt:
        print('\nexiting')
        exit(0)
