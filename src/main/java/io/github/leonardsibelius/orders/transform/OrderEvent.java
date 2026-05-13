package io.github.leonardsibelius.orders.transform;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderEvent(
        long orderId,
        String customerId,
        BigDecimal amount,
        String currency,
        Instant createdAt,
        Instant processedAt,
        String sourceSystem) {
}
