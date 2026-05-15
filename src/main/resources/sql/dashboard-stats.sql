-- Counts of orders by pipeline status, for the dashboard's state-counts card.
SELECT status, COUNT(*) AS n FROM orders GROUP BY status
