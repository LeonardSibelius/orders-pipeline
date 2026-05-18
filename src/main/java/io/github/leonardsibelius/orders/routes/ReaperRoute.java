package io.github.leonardsibelius.orders.routes;

import java.util.Map;

import org.apache.camel.Body;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.PropertyInject;
import org.apache.camel.builder.RouteBuilder;

/**
 * Restores rows stuck in {@code IN_PROGRESS} back to {@code NEW} so the
 * main pipeline picks them up again. A row gets stuck when a worker dies
 * between claiming it (in {@link OrderSyncRoute}) and the {@code onConsume}
 * callback marking it {@code SENT} -- without the reaper it would remain
 * {@code IN_PROGRESS} forever.
 *
 * <p>Three routes:
 * <ul>
 *   <li>{@code timer:reaper} -- polls Postgres for stuck rows on a configurable
 *       interval, splits, and dispatches each via choice().</li>
 *   <li>{@code direct:reaper-reclaim} -- flips the row back to NEW and
 *       increments {@code claim_count}.</li>
 *   <li>{@code direct:reaper-poison} -- marks the row ERROR with reason
 *       {@code stuck-too-many-times} once {@code claim_count} would exceed
 *       {@code reaper.max-claims}. Prevents infinite reclaim loops on
 *       genuinely bad rows.</li>
 * </ul>
 *
 * <p>Lives in its own RouteBuilder rather than alongside {@code OrderSyncRoute}
 * because it's a different concern (recovery vs. main flow) and a different
 * trigger (timer vs. SQL consumer). Same pattern as {@link DashboardRoute}.
 */
public class ReaperRoute extends RouteBuilder {

    /**
     * Cap on how many reclaim attempts a row gets before we give up and
     * mark it ERROR. Loaded from {@code application.properties} so the
     * smoke tests can lower it for the poison-row scenario.
     */
    @PropertyInject("reaper.max-claims")
    private int maxClaims;

    @Override
    public void configure() throws Exception {

        // Timer-driven discovery of stuck rows. Default period is 60s with a
        // 10s startup delay so the SQL consumer claims pre-existing NEW rows
        // before the reaper has anything to do.
        from("timer:reaper"
                + "?period={{reaper.interval-ms}}"
                + "&delay={{reaper.initial-delay-ms}}")
            .routeId("orders-reaper-find")
            .setHeader("stuckSeconds", simple("{{reaper.stuck-after-seconds}}", Long.class))
            .to("sql:classpath:sql/reaper-find-stuck.sql?dataSource=#ordersDataSource")
            .split(body())
                .process(ReaperRoute::extractRowHeaders)
                .choice()
                    .when(method(this, "isPoison"))
                        .to("direct:reaper-poison")
                    .otherwise()
                        .to("direct:reaper-reclaim")
                .end();

        from("direct:reaper-reclaim")
            .routeId("orders-reaper-reclaim")
            .to("sql:classpath:sql/reaper-reclaim.sql?dataSource=#ordersDataSource")
            .log(LoggingLevel.WARN,
                 "Reclaimed row id=${header.orderId} after stuck for ${header.stuckSecs} seconds, claim_count now ${header.newClaimCount}")
            // v1.3: record this reclaim in reclaim_log for the dashboard's
            // Reclaim Activity panel. doTry/doCatch isolates an INSERT
            // failure so it cannot undo the UPDATE that already succeeded
            // above -- and we WARN-log the missed event rather than failing
            // silently. The reclaim happened; the history line did not.
            .doTry()
                .to("sql:classpath:sql/reaper-log-reclaim.sql?dataSource=#ordersDataSource")
            .doCatch(Exception.class)
                .log(LoggingLevel.WARN,
                     "Failed to record reclaim event for id=${header.orderId} in reclaim_log: ${exception.message}. The reclaim itself succeeded; the sparkline will be missing this event.")
            .end();

        from("direct:reaper-poison")
            .routeId("orders-reaper-poison")
            .to("sql:classpath:sql/reaper-mark-poison.sql?dataSource=#ordersDataSource")
            .log(LoggingLevel.WARN,
                 "Marked row id=${header.orderId} as ERROR (stuck-too-many-times, claim_count now ${header.newClaimCount}, last stuck for ${header.stuckSecs} seconds)");
    }

    /**
     * Pulls the per-row values out of the stuck-rows result Map and
     * promotes them to headers so the downstream SQL endpoints (which
     * bind via {@code :?orderId}) and the WARN log expressions can read
     * them by name.
     */
    private static void extractRowHeaders(Exchange exchange) {
        @SuppressWarnings("unchecked")
        Map<String, Object> row = exchange.getMessage().getBody(Map.class);
        long id = ((Number) row.get("id")).longValue();
        int claimCount = ((Number) row.get("claim_count")).intValue();
        int stuckSecs = ((Number) row.get("stuck_secs")).intValue();
        exchange.getMessage().setHeader("orderId", id);
        exchange.getMessage().setHeader("currentClaimCount", claimCount);
        exchange.getMessage().setHeader("newClaimCount", claimCount + 1);
        exchange.getMessage().setHeader("stuckSecs", stuckSecs);
    }

    /**
     * Poison-row predicate -- typed Java, no Simple-language string
     * coercion. Called from {@code .when(method(this, "isPoison"))}.
     * Returns true when applying one more reclaim would push the row
     * past {@code reaper.max-claims}.
     */
    public boolean isPoison(@Body Map<String, Object> row) {
        int claimCount = ((Number) row.get("claim_count")).intValue();
        return claimCount + 1 > maxClaims;
    }
}
