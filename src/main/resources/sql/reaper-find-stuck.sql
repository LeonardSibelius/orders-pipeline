-- Find rows stuck IN_PROGRESS longer than :?stuckSeconds. Returns id,
-- current claim_count, and how long they've been stuck (so the reaper
-- can log a meaningful WARN per row).
SELECT id,
       claim_count,
       EXTRACT(EPOCH FROM (NOW() - claimed_at))::INT AS stuck_secs
FROM orders
WHERE status = 'IN_PROGRESS'
  AND claimed_at < NOW() - (:?stuckSeconds * INTERVAL '1 second')
