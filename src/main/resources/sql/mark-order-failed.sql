UPDATE orders SET status = 'ERROR', errored_at = NOW() WHERE id = :?orderId AND status = 'IN_PROGRESS'
