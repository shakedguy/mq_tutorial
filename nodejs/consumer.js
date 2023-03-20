import Redis from 'ioredis';
import * as dotenv from 'dotenv';

dotenv.config({ path: '../.env' });

const env = process.env;
const REDIS_PORT = env.REDIS_PORT;
const REDIS_DB = env.REDIS_DB;
const REDIS_HOST = env.REDIS_HOST;
const REDIS_PASSWORD = env.REDIS_PASSWORD;

const redis = new Redis({
	port: REDIS_PORT,
	host: REDIS_PORT,
	password: REDIS_PASSWORD,
	username: '',
	db: REDIS_DB,
});

// const redis = createClient({
// 	socket: {
// 		host: REDIS_HOST,
// 		port: REDIS_PORT,
// 	},
// 	database: REDIS_DB,
// 	password: REDIS_PASSWORD,
// });

redis.on('error', (err) => console.log('Redis Client Error', err));

const channelName = 'channel_name';

const consumer = async () => {
	console.log(`Waiting for messages on channel: "${channelName}"...`);
	await redis.brPop(
		[channelName, 'key2', 'some_other_key_1'],
		0,
		(err, data) => {
			if (err) {
				console.log(err);
			} else {
				console.log(data);
			}
		}
	);
};

const subscriber = async () => {
	console.log(`Waiting for messages on channel: "${channelName}"...`);
	await redis.subscribe(channelName, (err, count) => {
		if (err) {
			// Just like other commands, subscribe() can fail for some reasons,
			// ex network issues.
			console.error('Failed to subscribe: %s', err.message);
		} else {
			// `count` represents the number of channels this client are currently subscribed to.
			console.log(
				`Subscribed successfully! This client is currently subscribed to ${count} channels.`
			);
		}
	});

	redis.on('message', (channel, message) => {
		console.log(`Received ${message} from ${channel}`);
	});
};

// consumer();
// await subscriber();

redis.subscribe('my-channel-1', 'my-channel-2', (err, count) => {
	if (err) {
		// Just like other commands, subscribe() can fail for some reasons,
		// ex network issues.
		console.error('Failed to subscribe: %s', err.message);
	} else {
		// `count` represents the number of channels this client are currently subscribed to.
		console.log(
			`Subscribed successfully! This client is currently subscribed to ${count} channels.`
		);
	}
});

redis.on('message', (channel, message) => {
	console.log(`Received ${message} from ${channel}`);
});

// There's also an event called 'messageBuffer', which is the same as 'message' except
// it returns buffers instead of strings.
// It's useful when the messages are binary data.
redis.on('messageBuffer', (channel, message) => {
	// Both `channel` and `message` are buffers.
	console.log(channel, message);
});
