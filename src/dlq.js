// Tools for the Dead Letter Queue (DLQ) - the place where orders end up after failing too many times.
//
// There are two ways to empty the DLQ:
//   saveDlqToDatabase() - save every message to the `failed_orders` table, so a person can see what went wrong
//   requeueDlq()        - put every message back on the main queue, to process it again (after you fixed the bug)
const prisma = require('./db');
const config = require('./config');
const { sendPersistent } = require('./rabbit');

// Takes every message out of the DLQ and saves it to the failed_orders table.
// Returns how many messages were saved.
async function saveDlqToDatabase(channel) {
  let count = 0;

  while (true) {
    // channel.get() takes ONE message from the queue, or returns false when the queue is empty.
    const message = await channel.get(config.DLQ);
    if (!message) {
      break;
    }

    const order = JSON.parse(message.content.toString());

    // The consumer stored the last error message in a header when it retried the order.
    const headers = message.properties.headers || {};
    const reason = headers['x-last-error'] || 'unknown';

    // "upsert" = insert the row, or update it if this orderId is already in the table.
    await prisma.failedOrder.upsert({
      where: { orderId: order.orderId },
      create: { orderId: order.orderId, payload: order, reason },
      update: { reason, failedAt: new Date() },
    });

    // Only now tell RabbitMQ it may delete the message from the DLQ.
    channel.ack(message);
    count++;
  }

  return count;
}

// Takes every message out of the DLQ and sends it back to the main queue.
// Returns how many messages were sent back.
async function requeueDlq(channel) {
  let count = 0;

  while (true) {
    const message = await channel.get(config.DLQ);
    if (!message) {
      break;
    }

    sendPersistent(channel, config.MAIN_QUEUE, message.content, {
      messageId: message.properties.messageId,
    });

    channel.ack(message);
    count++;
  }

  return count;
}

module.exports = { saveDlqToDatabase, requeueDlq };
