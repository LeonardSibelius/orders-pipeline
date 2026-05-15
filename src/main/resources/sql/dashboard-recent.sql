-- Ten most recent orders for the dashboard's recent-activity table.
-- ORDER BY uses the latest event timestamp per row so the most recently
-- finalized (or in-flight) orders come first.
SELECT id, customer_id, amount, currency, status,
       created_at, claimed_at, sent_at, errored_at
FROM orders
ORDER BY COALESCE(sent_at, errored_at, claimed_at, created_at) DESC
LIMIT 10
