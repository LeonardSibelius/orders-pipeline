-- 24 hourly buckets of reclaim event counts for the sparkline.
-- generate_series produces all 24 hour boundaries, so a LEFT JOIN
-- against reclaim_log gives a row for EVERY hour, including those
-- with zero events (Postgres's GROUP BY without this would silently
-- drop empty buckets). Result: always 24 rows, oldest first.
WITH hours AS (
    SELECT generate_series(
        DATE_TRUNC('hour', NOW() - INTERVAL '23 hours'),
        DATE_TRUNC('hour', NOW()),
        INTERVAL '1 hour'
    ) AS bucket
)
SELECT hours.bucket,
       COALESCE(COUNT(reclaim_log.id), 0) AS n
FROM hours
LEFT JOIN reclaim_log
    ON DATE_TRUNC('hour', reclaim_log.reclaimed_at) = hours.bucket
GROUP BY hours.bucket
ORDER BY hours.bucket ASC
