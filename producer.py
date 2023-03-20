from os import getenv
from redis import Redis
from time import sleep
from dotenv import load_dotenv
import json

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


def producer():

    i = 1
    channel_name = 'report_tokens'
    print(f'Start producing messages on channel: "{channel_name}"...')
    while True:
        message = f'Message Number {i}'
        mq.rpush(channel_name, message)
        print(f'Produced Message Number {i}')
        i += 1


def publisher():

    i = 1
    channel_name = 'report_tokens'
    print(f'Start publishing messages on channel: "{channel_name}"...')
    message = {'action': 'start', 'number': 20}
    # message = {'action': 'start', 'number': 20}
    mq.publish(channel_name, json.dumps(message))
    # while True:
    #     message = f'Message Number {i}'
    #     mq.publish(channel_name, message)
    #     print(f'Publish Message Number {i}')
    #     i += 1
    #     sleep(1)


if __name__ == '__main__':

    try:
        # producer()
        publisher()
    except KeyboardInterrupt:
        print('\nexiting')
        exit(0)
