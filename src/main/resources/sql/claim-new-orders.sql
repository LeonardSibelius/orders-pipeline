-- Atomically claim up to 100 NEW orders for this consumer instance.
-- The inner FOR UPDATE SKIP LOCKED makes this multi-instance safe:
-- concurrent pollers each grab disjoint rows. The outer UPDATE flips
-- claimed rows to IN_PROGRESS so they aren't re-claimed before the
-- route can process them.
UPDATE orders
SET status = 'IN_PROGRESS',
    claimed_at = NOW()
WHERE id IN (
    SELECT id FROM orders
    WHERE status = 'NEW'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 100
)
RETURNING id, customer_id, amount, currency, created_at
