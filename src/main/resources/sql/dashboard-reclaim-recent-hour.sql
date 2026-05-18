-- Count of reclaims in the last 1 hour, for the dashboard's Reclaim
-- Activity panel headline number.
SELECT COUNT(*) AS n
FROM reclaim_log
WHERE reclaimed_at > NOW() - INTERVAL '1 hour'
