
const express = require('express');
const prisma = require('./db');
const config = require('./config');
const { connect, setupQueues } = require('./rabbit');
const { makeOrders, publishOrders } = require('./producer');
const { saveDlqToDatabase, requeueDlq } = require('./dlq');
const { startConsumer, stopConsumer } = require('./consumer');

let isShuttingDown = false;

// =====================================================================
// RabbitMQ connection
// =====================================================================
// The server uses ONE connection with ONE channel for everything:
// publishing orders, reading queue sizes, and emptying the DLQ.
//
// It is a "confirm channel": RabbitMQ tells us when each published message is safely stored.
//
// We connect only when we need to ("lazily"). If RabbitMQ restarts, the 'close' events below
// reset rabbitConnection / rabbitChannel to null, and the next HTTP request simply connects again.

let rabbitConnection = null;
let rabbitChannel = null;

async function getChannel() {
  if (!rabbitConnection) {
    rabbitConnection = await connect('server');
    rabbitConnection.on('close', () => {
      rabbitConnection = null;
      rabbitChannel = null;
    });
  }

  if (!rabbitChannel) {
    rabbitChannel = await rabbitConnection.createConfirmChannel();
    // RabbitMQ closes a channel after some errors (e.g. checking a queue that doesn't exist).
    // When that happens, forget it so we create a fresh one next time.
    rabbitChannel.on('close', () => {
      rabbitChannel = null;
    });
    await setupQueues(rabbitChannel);
  }

  return rabbitChannel;
}

// =====================================================================
// HTTP API
// =====================================================================
const app = express();
app.use(express.json()); // read JSON request bodies into req.body

// Express 4 does not catch errors thrown inside `async` route handlers.
// Wrap a handler with asyncRoute(...) so any error becomes a 503 JSON response instead of a crash.
function asyncRoute(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (error) {
      console.error(error);
      res.status(503).json({ error: error.message });
    }
  };
}

// POST /orders - add one order.
// Body: { "orderId": "A1", "amount": 99.5 }   (orderId is optional)
app.post('/orders', asyncRoute(async (req, res) => {
  const amount = Number(req.body.amount);
  if (Number.isNaN(amount)) {
    return res.status(400).json({ error: 'amount is required' });
  }

  const order = {
    orderId: req.body.orderId || `ORD-${Date.now()}`,
    amount,
  };

  const channel = await getChannel();
  await publishOrders(channel, [order]);

  // 202 Accepted = "we queued it", not "it is already saved in the database"
  res.status(202).json({ queued: order });
}));

// POST /orders/bulk - add many fake test orders.
// Body: { "count": 10000 }   (default 10,000, maximum 100,000)
app.post('/orders/bulk', asyncRoute(async (req, res) => {
  const count = Math.min(Number(req.body.count) || 10000, 100000);

  const channel = await getChannel();
  const startTime = Date.now();
  const queued = await publishOrders(channel, makeOrders(count));

  res.status(202).json({ queued, ms: Date.now() - startTime });
}));

// GET /orders?limit=20 - the most recently processed orders (maximum 500).
app.get('/orders', asyncRoute(async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 20, 500);

  const orders = await prisma.order.findMany({
    orderBy: { processedAt: 'desc' },
    take: limit,
  });

  res.json(orders);
}));

// GET /stats - queue sizes and database counts.
app.get('/stats', asyncRoute(async (req, res) => {
  const channel = await getChannel();

  // 1. How many messages are waiting in each queue, and how many consumers read from it.
  const queues = {};
  for (const queueName of [config.MAIN_QUEUE, config.RETRY_QUEUE, config.DLQ]) {
    const info = await channel.checkQueue(queueName);
    queues[queueName] = { messages: info.messageCount, consumers: info.consumerCount };
  }

  // 2. Database counts.
  const processedCount = await prisma.order.count();
  const failedCount = await prisma.failedOrder.count();
  const countsByConsumer = await prisma.order.groupBy({
    by: ['consumer'],
    _count: true,
    orderBy: { consumer: 'asc' },
  });

  const perConsumer = countsByConsumer.map((row) => ({
    consumer: row.consumer,
    count: row._count,
  }));

  res.json({
    queues,
    db: { processed: processedCount, failed: failedCount, perConsumer },
  });
}));

// GET /dlq - failed orders that were saved from the DLQ (newest first).
app.get('/dlq', asyncRoute(async (req, res) => {
  const failedOrders = await prisma.failedOrder.findMany({ orderBy: { failedAt: 'desc' } });
  res.json(failedOrders);
}));

// POST /dlq/save - move all DLQ messages into the failed_orders table.
app.post('/dlq/save', asyncRoute(async (req, res) => {
  const channel = await getChannel();
  const savedCount = await saveDlqToDatabase(channel);
  res.json({ saved: savedCount });
}));

// POST /dlq/requeue - send all DLQ messages back to the main queue (use after fixing the bug).
app.post('/dlq/requeue', asyncRoute(async (req, res) => {
  const channel = await getChannel();
  const requeuedCount = await requeueDlq(channel);
  res.json({ requeued: requeuedCount });
}));

// GET /health - is the server up, and is it connected to RabbitMQ?
app.get('/health', (req, res) => {
  res.json({ ok: true, rabbitmq: rabbitConnection !== null });
});

// =====================================================================
// Start and stop
// =====================================================================
const server = app.listen(config.PORT, async () => {
  console.log(`[server] http://localhost:${config.PORT}`);

  // Connect now so the queues exist before the consumer starts.
  // If RabbitMQ isn't up yet, that's OK: we'll connect on the first request.
  try {
    await getChannel();
  } catch (error) {
    console.error('[server] RabbitMQ not ready yet:', error.message);
  }

  // The consumer runs in this same process (it reconnects by itself if RabbitMQ is down).
  startConsumer();
});

// Graceful shutdown: stop the HTTP server, let the consumer finish its orders in progress, then exit.
async function shutdown() {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;
  console.log('[server] shutting down...');

  server.close(); // stop accepting new HTTP requests
  await stopConsumer();

  try {
    if (rabbitConnection) {
      await rabbitConnection.close();
    }
  } catch (error) {
    // Already closed - ignore.
  }
  await prisma.$disconnect();
  process.exit(0);
}

// SIGINT = Ctrl+C, SIGTERM = "please stop" (docker stop, kill, ...)
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
