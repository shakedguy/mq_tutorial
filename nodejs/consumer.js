import { createClient } from 'redis';
import * as dotenv from 'dotenv';

dotenv.config({ path: '../.env' });

const env = process.env;
const REDIS_PORT = env.REDIS_PORT;
const REDIS_DB = env.REDIS_DB;
const REDIS_HOST = env.REDIS_HOST;
const REDIS_PASSWORD = env.REDIS_PASSWORD;

const redis = createClient({
	socket: {
		host: REDIS_HOST,
		port: REDIS_PORT,
	},
	database: REDIS_DB,
	password: REDIS_PASSWORD,
});

const subscribe = async () => {
	const subscriber = redis.duplicate();

	await subscriber.connect();

	console.log(`Waiting for messages on channel: "${channel_name}"...`);

	await subscriber.subscribe('report_tokens', (message) => {
		console.log(message);
	});
};

subscribe();
