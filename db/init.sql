-- Schema for the orders-pipeline demo, mounted into the postgres container
-- at /docker-entrypoint-initdb.d/01-init.sql and executed on first start.

CREATE TABLE orders (
    id           BIGSERIAL PRIMARY KEY,
    customer_id  TEXT          NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    currency     CHAR(3)       NOT NULL,
    status       TEXT          NOT NULL DEFAULT 'NEW',
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    claimed_at   TIMESTAMPTZ,
    sent_at      TIMESTAMPTZ,
    errored_at   TIMESTAMPTZ,
    -- v1.2: reaper bookkeeping. claim_count is the number of times the
    -- reaper has reclaimed this row from a stuck IN_PROGRESS state.
    -- error_reason is set when the reaper gives up (e.g. 'stuck-too-many-times').
    claim_count  INT           NOT NULL DEFAULT 0,
    error_reason TEXT,
    CONSTRAINT orders_status_check
        CHECK (status IN ('NEW', 'IN_PROGRESS', 'SENT', 'ERROR'))
);

-- Partial index keeps the claim query fast as the table grows:
-- once a row is SENT (the common case), it drops out of the index.
CREATE INDEX orders_new_created_at_idx ON orders (created_at)
    WHERE status = 'NEW';

-- v1.3: per-reclaim event log used by the dashboard's Reclaim Activity
-- panel. The reaper INSERTs one row here on each successful reclaim
-- (the poison-row path does NOT log -- those are terminations, not
-- reclaims, so they don't belong in the reclaim history).
--
-- Write-once log: no UPDATE/DELETE expected, no FK to orders(id) by
-- design (cleanup is a v2.x concern alongside real migration tooling).
CREATE TABLE reclaim_log (
    id              BIGSERIAL   PRIMARY KEY,
    order_id        BIGINT      NOT NULL,
    reclaimed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    new_claim_count INT         NOT NULL
);

-- Supports both the last-hour count query and the 24-hour sparkline
-- aggregation (DATE_TRUNC('hour', reclaimed_at) over recent rows).
CREATE INDEX reclaim_log_reclaimed_at_idx ON reclaim_log (reclaimed_at);

-- Seed rows so `./mvnw camel:run` produces visible Kafka traffic immediately.
INSERT INTO orders (customer_id, amount, currency) VALUES
    ('cust-001', 49.99,  'USD'),
    ('cust-002', 129.50, 'EUR'),
    ('cust-003', 9.95,   'USD');
