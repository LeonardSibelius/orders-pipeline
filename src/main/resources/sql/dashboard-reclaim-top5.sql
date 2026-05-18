-- Top 5 orders by reclaim count for the dashboard. Filters out rows
-- that have never been reclaimed (claim_count = 0) -- those aren't
-- "reclaim activity". Secondary sort id ASC stabilizes ties so the
-- panel doesn't visually jitter on each refresh.
SELECT id, status, claim_count, claimed_at
FROM orders
WHERE claim_count > 0
ORDER BY claim_count DESC, id ASC
LIMIT 5
