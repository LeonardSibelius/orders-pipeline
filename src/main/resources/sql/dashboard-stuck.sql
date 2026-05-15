-- Count of rows stuck IN_PROGRESS for more than 5 minutes (i.e. consumer
-- crashed between claim and onConsume). The v1.2 reaper will recover
-- these automatically; v1.1 just surfaces them on the dashboard.
SELECT COUNT(*) AS n FROM orders
WHERE status = 'IN_PROGRESS'
  AND claimed_at < NOW() - INTERVAL '5 minutes'
