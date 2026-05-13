-- Schema for the orders-pipeline demo, mounted into the postgres container
-- at /docker-entrypoint-initdb.d/01-init.sql and executed on first start.

CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id TEXT          NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    currency    CHAR(3)       NOT NULL,
    status      TEXT          NOT NULL DEFAULT 'NEW',
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    claimed_at  TIMESTAMPTZ,
    sent_at     TIMESTAMPTZ,
    errored_at  TIMESTAMPTZ,
    CONSTRAINT orders_status_check
        CHECK (status IN ('NEW', 'IN_PROGRESS', 'SENT', 'ERROR'))
);

-- Partial index keeps the claim query fast as the table grows:
-- once a row is SENT (the common case), it drops out of the index.
CREATE INDEX orders_new_created_at_idx ON orders (created_at)
    WHERE status = 'NEW';

-- Seed rows so `mvn camel:run` produces visible Kafka traffic immediately.
INSERT INTO orders (customer_id, amount, currency) VALUES
    ('cust-001', 49.99,  'USD'),
    ('cust-002', 129.50, 'EUR'),
    ('cust-003', 9.95,   'USD');
