package io.github.leonardsibelius.orders.transform;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

import org.apache.camel.BindToRegistry;

@BindToRegistry("orderEnricher")
public class OrderEnricher {

    private static final String SOURCE_SYSTEM = "orders-pipeline";

    public OrderEvent toEvent(Map<String, Object> row) {
        return new OrderEvent(
                ((Number) row.get("id")).longValue(),
                String.valueOf(row.get("customer_id")),
                toBigDecimal(row.get("amount")),
                String.valueOf(row.get("currency")),
                toInstant(row.get("created_at")),
                Instant.now(),
                SOURCE_SYSTEM);
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        if (value instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        return new BigDecimal(value.toString());
    }

    private static Instant toInstant(Object value) {
        if (value instanceof Instant i) {
            return i;
        }
        if (value instanceof OffsetDateTime odt) {
            return odt.toInstant();
        }
        if (value instanceof java.sql.Timestamp ts) {
            return ts.toInstant();
        }
        throw new IllegalArgumentException(
                "Unexpected created_at type from JDBC: " + value.getClass());
    }
}
