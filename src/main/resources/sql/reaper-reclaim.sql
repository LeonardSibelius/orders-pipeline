-- Reclaim a stuck row: flip status back to NEW so the SQL consumer
-- picks it up again on its next poll, and bump claim_count for the
-- poison-row cutoff and the (planned v1.3) reclaim-activity panel.
-- The WHERE-clause status guard makes this a no-op if the row was
-- unstuck between SELECT and UPDATE (parallel reaper, SQL consumer,
-- or a human).
UPDATE orders
SET status = 'NEW',
    claimed_at = NULL,
    claim_count = claim_count + 1
WHERE id = :?orderId
  AND status = 'IN_PROGRESS'
