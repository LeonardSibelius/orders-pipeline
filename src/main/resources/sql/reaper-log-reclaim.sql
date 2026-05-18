-- Append a row to reclaim_log for the dashboard's Reclaim Activity panel.
-- Called by ReaperRoute on the reclaim path only; the poison path does
-- NOT log here (poison rows are terminations, not reclaims). reclaimed_at
-- defaults to NOW(); new_claim_count is the value AFTER the UPDATE bumped it.
INSERT INTO reclaim_log (order_id, new_claim_count)
VALUES (:?orderId, :?newClaimCount)
