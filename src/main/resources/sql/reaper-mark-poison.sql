-- Stop reclaiming this row -- it has been stuck too many times. Mark
-- it ERROR with an explanatory reason so the dashboard surfaces it
-- for human attention. claim_count is still bumped so the historical
-- record is accurate.
UPDATE orders
SET status = 'ERROR',
    errored_at = NOW(),
    error_reason = 'stuck-too-many-times',
    claim_count = claim_count + 1
WHERE id = :?orderId
  AND status = 'IN_PROGRESS'
