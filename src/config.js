// All settings in one place.
// Values that change per machine (URLs, port) come from the .env file.

module.exports = {
  // ---------- connection settings (from .env) ----------
  AMQP_URL: process.env.AMQP_URL || 'amqp://guest:guest@localhost:5680', // RabbitMQ address
  PORT: Number(process.env.PORT || 3080),                                  // HTTP port for the API

  // ---------- queue names ----------
  MAIN_QUEUE: 'orders.main',   // new orders go here and consumers read from here
  RETRY_QUEUE: 'orders.retry', // failed orders wait here for a few seconds, then go back to the main queue
  DLQ: 'orders.dlq',           // "Dead Letter Queue": orders that failed too many times end up here
  DLX: 'orders.dlx',           // "Dead Letter Exchange": RabbitMQ uses it to move rejected orders into the DLQ

  // ---------- limits ----------
  MAX_RETRIES: 3,              // how many times we retry a failed order before sending it to the DLQ
  RETRY_DELAY_MS: 5000,        // how long an order waits in the retry queue (5 seconds)
  MAX_QUEUE_LENGTH: 20000,     // the main queue refuses new orders when it holds this many (overload protection)
  CONSUMER_COUNT: Number(process.env.CONSUMER_COUNT || 5), // how many consumers read the main queue
  PREFETCH: 10,                // each consumer works on at most 10 orders at the same time (5 x 10 = 50 in total)
};
