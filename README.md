
```bash
docker compose -p rabit up -d   # RabbitMQ :5680 (UI :15680 guest/guest), Postgres :5440
npm install
npx prisma db push              # create tables from prisma/schema.prisma
npm start                       # server on http://localhost:3080 + the 5 consumers
```


Settings are in `.env` (DATABASE_URL, AMQP_URL, PORT). You can also set FAIL_RATE (default 0.02).

The server and the consumers share one Node process. It starts `CONSUMER_COUNT` (5) consumers, `consumer-1` to `consumer-5`, each with its own RabbitMQ connection; RabbitMQ shares the orders between them round-robin. Each one works on up to `PREFETCH` (10) orders at once, so 50 in total. Set `CONSUMER_COUNT` in `.env` to change the number. Trade-off: if the process crashes, the API and the consumers go down together. Run it under something that restarts it (docker `restart: always`, systemd, PM2).

## API

```bash
curl -XPOST localhost:3080/orders -H 'content-type: application/json' -d '{"orderId":"A1","amount":49.99}'
curl -XPOST localhost:3080/orders/bulk -H 'content-type: application/json' -d '{"count":10000}'
curl localhost:3080/stats          # queue sizes, processed/failed counts, per-consumer counts
curl localhost:3080/orders?limit=20
curl -XPOST localhost:3080/dlq/save     # DLQ -> failed_orders table
curl localhost:3080/dlq                 # list failed orders
curl -XPOST localhost:3080/dlq/requeue  # DLQ -> back to main queue (after fixing the bug)
curl localhost:3080/health
```

Ctrl+C stops the server gracefully: each consumer finishes its in-flight messages first.

## Files

| File | What it does |
|------|--------------|
| `src/server.js` | Express API; starts and stops the consumers in the same process |
| `src/consumer.js` | The 5 consumers (`startConsumer` / `stopConsumer` start and stop all of them): saves orders, acks, retries, sends to the DLQ, reconnects |
| `src/producer.js` | Publishes orders with confirms; resends when the queue is full |
| `src/dlq.js` | Empties the DLQ (save to DB or requeue) |
| `src/rabbit.js` | Shared RabbitMQ helpers: connect, queue setup, persistent send |
| `src/db.js` | Prisma client |
| `src/config.js` | Queue names and limits |
| `prisma/schema.prisma` | `orders` and `failed_orders` tables |

## How each problem is handled

| # | Problem | Solution | Where |
|---|---------|----------|-------|
| 1 | RabbitMQ crash/restart | `durable: true` queues + `persistent: true` messages + docker volume, so nothing is lost. Publisher confirms mean the producer knows RabbitMQ really stored each message. Each consumer reconnects automatically every 5s, and the API reconnects on the next request. | `rabbit.js`, `producer.js`, `consumer.js` `scheduleReconnect()` |
| 2 | Ack | Manual ack, sent **only after** the Postgres insert succeeds. If the process dies before acking, RabbitMQ delivers the message again after restart. | `consumer.js` `handleMessage()` |
| 3 | Queue overload/full | `x-max-length` + `x-overflow: reject-publish` caps the queue; the producer gets a nack, waits, and resends. `prefetch(10)` means each of the 5 consumers holds at most 10 messages (50 in total), so memory stays flat. | `rabbit.js`, `producer.js`, `consumer.js` |
| 4 | Auto ack | **Don't use it for orders.** With `noAck: true`, RabbitMQ deletes the message as soon as it is delivered; if processing fails or the consumer crashes, the order is gone. Try it: `AUTO_ACK=1 npm start` (it logs `LOST`). It's fine only for data you can afford to lose (logs, metrics). | `consumer.js` |
| 5 | Dead Letter Queue | Temporary error: retry up to 3 times with a 5s delay (retry queue + TTL). Still failing: `nack(requeue=false)` sends it to `orders.dlq` through the dead-letter exchange. Then inspect it or requeue it. | `rabbit.js`, `consumer.js` `handleFailure()`, `dlq.js` |

## Other things that matter

- **Idempotency (most important):** RabbitMQ delivers *at least once*, so duplicates will happen (for example after a restart). `orderId @id` in the Prisma schema + `createMany({ skipDuplicates: true })` (= `ON CONFLICT DO NOTHING`) makes a duplicate harmless.
- **Graceful shutdown:** on Ctrl+C or `docker stop`, each consumer stops taking new messages, finishes the ones in progress, then exits.
- **No infinite requeue loops:** never call `nack(msg, false, true)` on an error that will keep happening; it spins forever. Use retry count + DLQ.
- **Heartbeat:** `heartbeat: 30` detects dead connections quickly.
- **Monitoring:** check queue depth / unacked / DLQ size in the UI at http://localhost:15680, or with `docker exec rabit-rabbitmq-1 rabbitmqctl list_queues name messages messages_unacknowledged`.
- **Next step, if you need more safety:** quorum queues (`x-queue-type: quorum`) on a 3-node cluster survive a whole node being lost.

## Test result (earlier 5-process setup; POST /orders/bulk 10,000 + 1 single order, RabbitMQ restarted during processing)

- 9,991 orders saved, 0 duplicates. 10 bad orders (`amount < 0`) went to the DLQ; `/dlq/save` stored all 10.
- All 5 consumers reconnected after the RabbitMQ restart, and each processed about 1,700 to 2,100 orders.
- A consumer killed with `kill -9` was restarted by the server within 2 seconds.

