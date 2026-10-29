// Consumer = READS orders from RabbitMQ and saves them to Postgres.
//
// It runs inside the server process: server.js calls startConsumer() at startup and
// stopConsumer() on shutdown. We start CONSUMER_COUNT consumers (consumer-1, consumer-2, ...).
// Each one has its own connection and channel, and works on up to PREFETCH orders at the same time.
// RabbitMQ shares the orders of the main queue between them (round-robin).
// Trade-off: if the process crashes, the API and the consumers go down together.
//
// Life of one order:
//   1. RabbitMQ delivers the order to one of the consumers.
//   2. That consumer saves it to the database.
//   3. Success -> it "acks" it (tells RabbitMQ: done, you can delete it).
//      Failure -> it sends it to the retry queue, or to the DLQ if it already failed MAX_RETRIES times.
const prisma = require('./db');
const config = require('./config');
const { sleep, connect, setupQueues, sendPersistent } = require('./rabbit');

// ---------- settings ----------

// AUTO_ACK=1 is for demos only: it shows how orders get LOST with auto-ack. Never use it for real orders.
const AUTO_ACK = process.env.AUTO_ACK === '1';

// Chance of a fake random error (0.02 = 2%), so you can watch the retry logic work.
const FAIL_RATE = Number(process.env.FAIL_RATE || 0.02);

const RECONNECT_DELAY_MS = 5000;

// Creates one consumer. `name` shows up in logs and in the database (orders.consumer column).
function createConsumer(name) {
  // ---------- state (one set per consumer) ----------
  let connection = null;
  let channel = null;
  let consumerTag = null;      // id RabbitMQ gives our subscription; needed to stop it on shutdown
  let reconnectTimer = null;   // set while a reconnect is scheduled
  let ordersInProgress = 0;    // orders we started but have not finished yet
  let isShuttingDown = false;

  // ---------- processing ----------

  // Saves one order to the database. Throws an error if the order cannot be saved.
  async function saveOrder(order) {
    // A permanent error: retrying will never fix it, so this order will end up in the DLQ.
    if (order.amount < 0) {
      throw new Error('Invalid amount');
    }

    // A fake temporary error, to test retries.
    if (Math.random() < FAIL_RATE) {
      throw new Error('Random temporary failure');
    }

    // RabbitMQ can deliver the same order twice (for example after a crash).
    // orderId is the primary key, and skipDuplicates: true means "if this orderId already exists, do nothing".
    // So saving the same order twice is harmless. This is called being "idempotent".
    await prisma.order.createMany({
      data: [{ orderId: order.orderId, amount: order.amount, consumer: name }],
      skipDuplicates: true,
    });
  }

  // Decides what to do with an order that failed: retry it later, or give up and send it to the DLQ.
  function handleFailure(message, order, error) {
    if (AUTO_ACK) {
      // With auto-ack, RabbitMQ deleted the message the moment it gave it to us. It is gone.
      console.error(`[${name}] LOST ${order.orderId}: ${error.message} (auto-ack, cannot retry)`);
      return;
    }

    // We keep the retry count in a message header called "x-retries".
    const headers = message.properties.headers || {};
    const retries = headers['x-retries'] || 0;

    if (retries < config.MAX_RETRIES) {
      // Try again later: put a copy in the retry queue (it comes back to the main queue after RETRY_DELAY_MS)...
      sendPersistent(channel, config.RETRY_QUEUE, message.content, {
        messageId: message.properties.messageId,
        headers: { 'x-retries': retries + 1, 'x-last-error': error.message },
      });
      // ...and ack the original, because the copy now takes its place.
      channel.ack(message);
      console.warn(`[${name}] retry ${retries + 1}/${config.MAX_RETRIES} ${order.orderId}: ${error.message}`);
    } else {
      // Give up. nack(message, allUpTo = false, requeue = false) rejects the message WITHOUT putting it back.
      // Because the main queue has a dead-letter exchange, RabbitMQ moves it to the DLQ for us.
      channel.nack(message, false, false);
      console.error(`[${name}] DLQ ${order.orderId}: ${error.message}`);
    }
  }

  // Called by RabbitMQ for every message delivered to this consumer.
  async function handleMessage(message) {
    // RabbitMQ sends `null` when it cancels our subscription (for example, the queue was deleted).
    if (!message) {
      return;
    }

    ordersInProgress++;
    const order = JSON.parse(message.content.toString());

    try {
      await saveOrder(order);

      // Ack ONLY after the database write succeeded.
      // If we crash before this line, RabbitMQ never got the ack and delivers the order again after we restart.
      if (!AUTO_ACK) {
        channel.ack(message);
      }
    } catch (error) {
      handleFailure(message, order, error);
    } finally {
      ordersInProgress--;
    }
  }

  // ---------- connecting ----------

  // If RabbitMQ goes down (crash or restart), try to connect again in a few seconds.
  // Messages we had not acked yet are not lost - RabbitMQ delivers them again.
  //
  // Only one reconnect is scheduled at a time. A single failure can cause both an error AND a
  // "close" event, and without this check we would connect twice.
  function scheduleReconnect(reason) {
    if (isShuttingDown || reconnectTimer) {
      return;
    }

    console.error(`[${name}] ${reason}, reconnecting in ${RECONNECT_DELAY_MS / 1000}s...`);

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      start();
    }, RECONNECT_DELAY_MS);
  }

  // Connects to RabbitMQ and starts receiving orders.
  async function start() {
    try {
      connection = await connect(name);
      connection.on('close', () => scheduleReconnect('connection lost'));

      channel = await connection.createChannel();
      await setupQueues(channel);

      // Prefetch: RabbitMQ gives this consumer at most PREFETCH un-acked orders at a time.
      // This caps how many orders we work on at once and keeps memory use low.
      await channel.prefetch(config.PREFETCH);

      // Start listening. handleMessage() is called for every order.
      // noAck: false (the normal case) means we must ack each message ourselves.
      const subscription = await channel.consume(config.MAIN_QUEUE, handleMessage, { noAck: AUTO_ACK });
      consumerTag = subscription.consumerTag;

      console.log(`[${name}] consuming (autoAck=${AUTO_ACK}, prefetch=${config.PREFETCH})`);
    } catch (error) {
      scheduleReconnect(`cannot connect (${error.message})`);
    }
  }

  // ---------- stopping ----------

  // Graceful stop: stop taking new orders and finish the ones in progress.
  async function stop() {
    if (isShuttingDown) {
      return;
    }
    isShuttingDown = true;
    clearTimeout(reconnectTimer);

    console.log(`[${name}] shutting down, waiting for ${ordersInProgress} orders in progress...`);

    // 1. Tell RabbitMQ to stop sending us new orders.
    try {
      if (consumerTag) {
        await channel.cancel(consumerTag);
      }
    } catch (error) {
      // The connection may already be gone - that's fine, we are exiting anyway.
    }

    // 2. Wait until the orders we already started are finished.
    while (ordersInProgress > 0) {
      await sleep(100);
    }

    // 3. Close our RabbitMQ connection.
    try {
      if (connection) {
        await connection.close();
      }
    } catch (error) {
      // Already closed - ignore.
    }
  }

  return { start, stop };
}

// ---------- all consumers ----------

const consumers = [];

// Starts CONSUMER_COUNT consumers: consumer-1, consumer-2, ...
function startConsumer() {
  for (let i = 1; i <= config.CONSUMER_COUNT; i++) {
    const consumer = createConsumer(`consumer-${i}`);
    consumers.push(consumer);
    consumer.start();
  }
}

// Stops all consumers at the same time. The server closes the database afterwards.
async function stopConsumer() {
  await Promise.all(consumers.map((consumer) => consumer.stop()));
}

module.exports = { startConsumer, stopConsumer };
