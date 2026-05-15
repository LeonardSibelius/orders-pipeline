# orders-pipeline

> A Spring-Boot-free integration pipeline: Postgres → Apache Camel → Kafka.
> Built on `camel-main` to show idiomatic Apache Camel 4 without Spring on the classpath.

## Architecture

```
┌─────────────────────────┐       (every 2s)
│  Postgres               │ ──── claim NEW rows ────┐
│  orders table           │                         │
│  state machine:         │                         ▼
│  NEW → IN_PROGRESS →    │     ┌──────────────────────────────────┐
│  SENT or ERROR          │     │  Route: orders-postgres-to-kafka │
└─────────────────────────┘     │                                  │
            ▲                   │  1. SQL consumer (atomic claim)  │
            │                   │  2. OrderEnricher: Map → POJO    │
   onConsume│                   │  3. Jackson marshal: POJO → JSON │
   onConsumeFailed              │  4. Kafka producer               │
            │                   │                                  │
            └───────────────────┤  onConsume:       UPDATE → SENT  │
                                │  onConsumeFailed: UPDATE → ERROR │
                                └──────────────────┬───────────────┘
                                                   │
                                ┌──────────────────▼───────────────┐
                                │  Kafka topic: orders.new         │
                                │  key   = order id (partitioning) │
                                │  value = enriched JSON           │
                                └──────────────────────────────────┘
                                + DLQ: orders.dlq for poison messages
```

## Tech stack

| Layer            | Technology                       | Notes                                            |
|------------------|----------------------------------|--------------------------------------------------|
| JVM              | Java 17                          | LTS; Camel 4.x baseline                          |
| Integration      | Apache Camel 4.10                | `camel-main` standalone — no Spring              |
| DB source        | `camel-sql` + PostgreSQL 16      | Atomic-claim `UPDATE … RETURNING` pattern        |
| Connection pool  | HikariCP                         | Lifecycle owned by the Camel registry            |
| Sink             | `camel-kafka`                    | Idempotent producer, `acks=all`                  |
| Payload (v1)     | JSON via `camel-jackson`         | Avro + Schema Registry planned for v2            |
| Broker (dev)     | Redpanda 24.2                    | Single binary, Kafka-API-compatible              |
| UI (dev)         | Provectus Kafka UI               | Topic + message browser                          |
| Observability    | Camel dev console + Jolokia + health | Built-in HTTP surface, no extra deps         |

## Quick start

```bash
# 1. Start the local infrastructure (Postgres + Redpanda + Kafka UI)
docker compose up -d --wait

# 2. Run the pipeline (blocks until SIGTERM)
./mvnw camel:run
```

Within ~2 seconds the three seed orders flow to Kafka. Inspect a running app via:

- Camel dev console — <http://localhost:8080/q/dev>
- Health checks — <http://localhost:8080/observe/health>
- Kafka UI — <http://localhost:8081> → cluster `local` → topic `orders.new`

## Verifying end-to-end

```bash
# Watch all orders flip to SENT
psql -h localhost -U orders -d orders \
     -c "SELECT id, status, sent_at FROM orders ORDER BY id"

# Add a new order, then re-query — status flips to SENT within ~2s
psql -h localhost -U orders -d orders \
     -c "INSERT INTO orders (customer_id, amount, currency)
         VALUES ('cust-099', 19.99, 'USD')"

# Or tail the topic from the CLI
kcat -b localhost:9092 -t orders.new -C -o end -f '%k → %s\n'
```

### Failure-path smoke test

```bash
# 1. Take the broker down, then add an order
docker compose stop redpanda
psql -h localhost -U orders -d orders \
     -c "INSERT INTO orders (customer_id, amount, currency)
         VALUES ('cust-bad', 1.00, 'USD')"

# The route claims the row; the publish to orders.new fails fast (~5s),
# retries 3× with 2s backoff, the DLQ write to orders.dlq also fails,
# and onConsumeFailed marks the row ERROR -- recoverable, not lost:
psql -h localhost -U orders -d orders \
     -c "SELECT id, status FROM orders WHERE customer_id = 'cust-bad'"
#  -> status = ERROR

# 2. Bring the broker back and requeue the row
#    (this manual UPDATE is what the planned v1.2 reaper will automate)
docker compose start redpanda
psql -h localhost -U orders -d orders \
     -c "UPDATE orders SET status='NEW', claimed_at=NULL, errored_at=NULL
         WHERE customer_id = 'cust-bad' AND status = 'ERROR'"
# Within one ~2s poll cycle it flows through to orders.new and SENT.
```

## Design notes

### Why `camel-main`, not Spring Boot?

This is a single-purpose integration process — one route, one DB, one broker. Spring Boot's auto-configuration solves problems we don't have, while `camel-main` gives us classpath scanning, properties, lifecycle, dev console, health checks, JMX-over-HTTP, and CLI attachment without the dependency surface. There are zero Spring artifacts on the classpath.

### Why an atomic-claim `UPDATE … RETURNING`, not `SELECT … FOR UPDATE`?

A plain `SELECT … FOR UPDATE SKIP LOCKED` is multi-instance safe only if the SELECT and the subsequent UPDATE share a transaction — otherwise the lock is released the instant the SELECT returns and concurrent pollers race for the same rows. Sharing a real transaction in `camel-sql` requires a Spring `PlatformTransactionManager`. To stay Spring-free, we use Postgres's atomic UPDATE-with-RETURNING idiom in [`claim-new-orders.sql`](src/main/resources/sql/claim-new-orders.sql): a single statement that flips status to `IN_PROGRESS` *and* returns the row data. The `FOR UPDATE SKIP LOCKED` inside the subquery still makes the row selection concurrency-safe; the outer UPDATE makes the claim durable in one statement.

### DLQ-first error handling

The route-level `errorHandler(deadLetterChannel(...))` retries transient failures 3× with 2s backoff, then routes the failed exchange to `orders.dlq`. When that DLQ write **succeeds**, `deadLetterChannel` marks the exchange handled, so `camel-sql`'s `onConsume` fires and the row flips to `SENT` — the DLQ topic, not Postgres, is where downstream investigation happens.

When the broker is *fully* unavailable the DLQ write fails too. `.deadLetterHandleNewException(false)` makes that second failure propagate instead of being swallowed, so `camel-sql` runs `onConsumeFailed` and the row is marked `ERROR` — recoverable by requeuing it to `NEW`, never silently `SENT` while the event was lost. Kafka producer timeouts are tuned short (`delivery-timeout-ms=10s`) so this path resolves in seconds rather than minutes.

### Why `OrderEvent` doesn't carry `status`

The DB `status` column tracks the pipeline's *internal* lifecycle (`NEW` / `IN_PROGRESS` / `SENT` / `ERROR`) — it's a producer concern, not an order property. Including it would mean every Kafka event carries `"status": "IN_PROGRESS"`, which is misleading: by the time a consumer sees the message, the row is already `SENT`. The published event represents the order, not the pipeline. A future business-level status (e.g. `PENDING` / `CONFIRMED` / `SHIPPED`) would live in a separate column and *would* belong on `OrderEvent`.

### The orders state machine

```
              ┌────────┐    claim     ┌─────────────┐   onConsume     ┌────────┐
INSERT  ───►  │  NEW   │  ──────────► │ IN_PROGRESS │ ──────────────► │  SENT  │
              └────────┘              └─────────────┘                 └────────┘
                  ▲                          │
                  │                          │ onConsumeFailed
   stuck-row      │                          │ (catastrophe)
   reaper (v1.2)  │                          ▼
                  │                     ┌─────────┐
                  └─────────────────────│  ERROR  │
                                        └─────────┘
```

## v1 known limits

- **Stuck-`IN_PROGRESS` rows on consumer crash.** If a pipeline instance dies between claiming rows and the route completing, those rows stay `IN_PROGRESS` until manually reset. A reaper that resets rows whose `claimed_at` is older than ~5 minutes is the planned **v1.2** commit.
- **JSON payload.** Friendly for `kcat` debugging, less friendly for schema evolution. **v2.0** swaps to Avro + Confluent Schema Registry.
- **No metrics export.** Camel's dev console gives runtime introspection; Micrometer + Prometheus is a **v2.1** candidate.

## Roadmap

| Tag  | Change                                       | Why                                                |
|------|----------------------------------------------|----------------------------------------------------|
| v1.0 | *(this)* — working end-to-end on JSON        | Show the canonical shape of a `camel-main` pipeline |
| v1.1 | Operator dashboard (HTMX + Tailwind)         | At-a-glance pipeline state, served by camel-main from the same JVM |
| v1.2 | Stuck-`IN_PROGRESS` reaper route             | Recover from consumer crashes without manual fix   |
| v2.0 | Avro + Confluent Schema Registry             | Schema-evolution discipline on the wire format     |
| v2.1 | Micrometer metrics + Grafana dashboard       | Per-route latency / throughput / DLQ-rate panels   |

The git history is intentionally narrative — each tag above will be its own series of small commits that show why the change was needed.

## File layout

```
orders-pipeline/
├── pom.xml                              # camel-main + camel-sql + camel-kafka + camel-jackson
├── docker-compose.yml                   # Postgres + Redpanda + Kafka UI for local dev
├── db/
│   └── init.sql                         # orders schema + partial index + seed rows
└── src/main/
    ├── java/io/github/leonardsibelius/orders/
    │   ├── PipelineApplication.java     # camel-main bootstrap
    │   ├── PipelineConfiguration.java   # @BindToRegistry: DataSource + ObjectMapper
    │   ├── routes/
    │   │   └── OrderSyncRoute.java      # the pipeline route
    │   └── transform/
    │       ├── OrderEvent.java          # Kafka payload (Java 17 record)
    │       └── OrderEnricher.java       # Map<String,Object> → OrderEvent
    └── resources/
        ├── application.properties       # all runtime config
        ├── log4j2.properties            # logging
        └── sql/
            ├── claim-new-orders.sql     # UPDATE…RETURNING atomic claim
            ├── mark-order-sent.sql      # onConsume:       IN_PROGRESS → SENT
            └── mark-order-failed.sql    # onConsumeFailed: IN_PROGRESS → ERROR
```

## License

MIT — see [LICENSE](LICENSE).

This project was built with [Claude Code](https://claude.com/claude-code) (Anthropic) as a pair-programming collaborator. Design decisions and trade-offs are recorded in the commit messages.
