// Shared RabbitMQ helpers used by the server and the consumer.
const amqp = require('amqplib');
const c = require('./config');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function connect(name) {
  const conn = await amqp.connect(c.AMQP_URL, { heartbeat: 30 }); // heartbeat detects dead connections fast
  conn.on('error', (e) => console.error(`[${name}] amqp error:`, e.message));
  return conn;
}

// Creates exchanges and queues. Safe to call many times (assert = create if missing).
async function setupQueues(ch) {
  // DLQ: messages rejected from the main queue land here
  await ch.assertExchange(c.DLX, 'fanout', { durable: true });
  await ch.assertQueue(c.DLQ, { durable: true });
  await ch.bindQueue(c.DLQ, c.DLX, '');

  await ch.assertQueue(c.MAIN_QUEUE, {
    durable: true,                         // (1) queue survives RabbitMQ restart
    arguments: {
      'x-dead-letter-exchange': c.DLX,     // (5) nack(requeue=false) -> DLQ
      'x-max-length': c.MAX_QUEUE_LENGTH,  // (3) cap queue size
      'x-overflow': 'reject-publish',      // (3) when full, refuse new msgs (producer gets nack) instead of dropping old ones
    },
  });

  // Retry queue has no consumer: msgs wait RETRY_DELAY_MS, then expire back into the main queue
  await ch.assertQueue(c.RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': c.RETRY_DELAY_MS,
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': c.MAIN_QUEUE,
    },
  });
}

// (1) persistent = written to disk, survives RabbitMQ restart.
// On a confirm channel, `onConfirm(err)` is called once the broker stored (or rejected) the message.
function sendPersistent(ch, queue, content, options = {}, onConfirm) {
  return ch.sendToQueue(queue, content, { persistent: true, ...options }, onConfirm);
}

module.exports = { sleep, connect, setupQueues, sendPersistent };
