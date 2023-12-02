import { createClient } from 'redis';
import { Queue, Worker } from 'bullmq';

const env = process.env;
const REDIS_HOST = env.REDIS_HOST | 'localhost';
const REDIS_PASSWORD = env.REDIS_PASSWORD | '';

const queue = new Queue('test', {
	connection: {
		host: REDIS_HOST,
		password: REDIS_PASSWORD,
	},
});

queue.add('test', { foo: 'bar' });
