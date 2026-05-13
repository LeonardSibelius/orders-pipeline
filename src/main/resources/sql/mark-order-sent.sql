UPDATE orders SET status = 'SENT', sent_at = NOW() WHERE id = :?orderId AND status = 'IN_PROGRESS'
