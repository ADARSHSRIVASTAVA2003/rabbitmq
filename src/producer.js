// Producer = the code that PUTS orders into RabbitMQ.
//
// It is careful not to lose orders:
//   - every message is persistent (saved to disk)
//   - it waits for RabbitMQ to confirm each message was stored ("publisher confirms")
//   - if the queue is full and RabbitMQ refuses some messages, it waits and sends those again
const config = require('./config');
const { sleep, sendPersistent } = require('./rabbit');

const BATCH_SIZE = 500;          // send this many orders, wait for all confirms, then send the next batch
const QUEUE_FULL_WAIT_MS = 2000; // how long to wait before resending refused orders

// Creates `count` fake orders for testing.
// Every 1000th order has a negative amount. That is a "bad" order on purpose,
// so you can watch it fail, get retried, and finally land in the DLQ.
function makeOrders(count, prefix = `ORD-${Date.now()}`) {
  const orders = [];

  for (let number = 1; number <= count; number++) {
    const isBadOrder = number % 1000 === 0;
    const randomAmount = Math.round(Math.random() * 10000) / 100; // 0.00 to 100.00

    orders.push({
      orderId: `${prefix}-${number}`,
      amount: isBadOrder ? -1 : randomAmount,
    });
  }

  return orders;
}

// Sends one order and returns a Promise that:
//   - resolves when RabbitMQ confirms it stored the order
//   - rejects when RabbitMQ refuses it (for example: the queue is full)
function publishConfirmed(channel, order) {
  return new Promise((resolve, reject) => {
    const body = Buffer.from(JSON.stringify(order));
    const options = { messageId: order.orderId };

    sendPersistent(channel, config.MAIN_QUEUE, body, options, (error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

// Sends a list of orders and only returns once RabbitMQ has stored ALL of them.
// `channel` must be a confirm channel (created with connection.createConfirmChannel()).
async function publishOrders(channel, orders) {
  for (let start = 0; start < orders.length; start += BATCH_SIZE) {
    let ordersToSend = orders.slice(start, start + BATCH_SIZE);

    // Keep going until every order in this batch was accepted.
    while (ordersToSend.length > 0) {
      // Send the whole batch at once and wait until every order has an answer (accepted or refused).
      const results = await Promise.allSettled(
        ordersToSend.map((order) => publishConfirmed(channel, order))
      );

      // Keep only the orders RabbitMQ refused - those are the ones we must send again.
      const refusedOrders = [];
      for (let i = 0; i < ordersToSend.length; i++) {
        if (results[i].status === 'rejected') {
          refusedOrders.push(ordersToSend[i]);
        }
      }
      ordersToSend = refusedOrders;

      if (ordersToSend.length > 0) {
        console.log(`Queue full, ${ordersToSend.length} refused, waiting ${QUEUE_FULL_WAIT_MS}ms...`);
        await sleep(QUEUE_FULL_WAIT_MS);
      }
    }
  }

  return orders.length;
}

module.exports = { makeOrders, publishOrders };
