package io.github.leonardsibelius.orders.routes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import io.github.leonardsibelius.orders.dashboard.DashboardRenderer;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

public class DashboardRoute extends RouteBuilder {

    private static final String CONTENT_TYPE_HTML = "text/html; charset=utf-8";

    @Override
    public void configure() throws Exception {

        // Loaded once at startup; the file does not change at runtime.
        String indexHtml = loadResource("static/index.html");

        // GET /dashboard -- the single-page operator UI.
        from("platform-http:/dashboard?httpMethodRestrict=GET")
                .routeId("dashboard-page")
                .setBody(constant(indexHtml))
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));

        // GET /api/stats -- HTML fragment with order counts by status.
        from("platform-http:/api/stats?httpMethodRestrict=GET")
                .routeId("dashboard-api-stats")
                .to("sql:classpath:sql/dashboard-stats.sql?dataSource=#ordersDataSource")
                .process(DashboardRoute::renderStats)
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));

        // GET /api/recent -- HTML table of the 10 most recent orders.
        from("platform-http:/api/recent?httpMethodRestrict=GET")
                .routeId("dashboard-api-recent")
                .to("sql:classpath:sql/dashboard-recent.sql?dataSource=#ordersDataSource")
                .process(DashboardRoute::renderRecent)
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));

        // GET /api/dlq-size -- HTML fragment with the orders.dlq message count.
        from("platform-http:/api/dlq-size?httpMethodRestrict=GET")
                .routeId("dashboard-api-dlq-size")
                .setBody(constant("orders.dlq"))
                .bean("kafkaTopicStats", "messagesIn")
                .process(DashboardRoute::renderDlqSize)
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));

        // GET /api/stuck -- HTML fragment, red banner if any IN_PROGRESS rows
        //                  are older than five minutes.
        from("platform-http:/api/stuck?httpMethodRestrict=GET")
                .routeId("dashboard-api-stuck")
                .to("sql:classpath:sql/dashboard-stuck.sql?dataSource=#ordersDataSource")
                .process(DashboardRoute::renderStuck)
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));

        // GET /api/health -- HTML fragment with reachability indicators for
        //                   Postgres, Kafka, and the Camel context itself.
        from("platform-http:/api/health?httpMethodRestrict=GET")
                .routeId("dashboard-api-health")
                .bean("healthChecker", "check")
                .process(DashboardRoute::renderHealth)
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));

        // GET /api/reclaim-activity -- HTML fragment with the last-hour
        //                              reclaim count, the top-5 orders by
        //                              claim_count, and a 24h sparkline.
        //                              Three SQL queries run sequentially;
        //                              each result is stashed as an exchange
        //                              property so the next .to() can run
        //                              without clobbering.
        from("platform-http:/api/reclaim-activity?httpMethodRestrict=GET")
                .routeId("dashboard-api-reclaim-activity")
                .to("sql:classpath:sql/dashboard-reclaim-recent-hour.sql?dataSource=#ordersDataSource")
                .setProperty("reclaimRecentHour", body())
                .to("sql:classpath:sql/dashboard-reclaim-top5.sql?dataSource=#ordersDataSource")
                .setProperty("reclaimTop5", body())
                .to("sql:classpath:sql/dashboard-reclaim-sparkline.sql?dataSource=#ordersDataSource")
                .setProperty("reclaimSparkline", body())
                .process(DashboardRoute::renderReclaimActivity)
                .setHeader(Exchange.CONTENT_TYPE, constant(CONTENT_TYPE_HTML));
    }

    // Bridge methods: pull typed inputs out of the exchange, delegate HTML
    // generation to DashboardRenderer (pure, no Camel types in its API).

    @SuppressWarnings("unchecked")
    private static void renderStats(Exchange exchange) {
        List<Map<String, Object>> rows = exchange.getMessage().getBody(List.class);
        exchange.getMessage().setBody(DashboardRenderer.stats(rows));
    }

    @SuppressWarnings("unchecked")
    private static void renderRecent(Exchange exchange) {
        List<Map<String, Object>> rows = exchange.getMessage().getBody(List.class);
        exchange.getMessage().setBody(DashboardRenderer.recent(rows));
    }

    private static void renderDlqSize(Exchange exchange) {
        long count = exchange.getMessage().getBody(Long.class);
        exchange.getMessage().setBody(DashboardRenderer.dlqSize(count));
    }

    @SuppressWarnings("unchecked")
    private static void renderStuck(Exchange exchange) {
        List<Map<String, Object>> rows = exchange.getMessage().getBody(List.class);
        exchange.getMessage().setBody(DashboardRenderer.stuck(rows));
    }

    @SuppressWarnings("unchecked")
    private static void renderHealth(Exchange exchange) {
        Map<String, Boolean> health = exchange.getMessage().getBody(Map.class);
        exchange.getMessage().setBody(DashboardRenderer.health(health));
    }

    @SuppressWarnings("unchecked")
    private static void renderReclaimActivity(Exchange exchange) {
        List<Map<String, Object>> recentHourRows =
                (List<Map<String, Object>>) exchange.getProperty("reclaimRecentHour");
        List<Map<String, Object>> top5 =
                (List<Map<String, Object>>) exchange.getProperty("reclaimTop5");
        List<Map<String, Object>> sparkline =
                (List<Map<String, Object>>) exchange.getProperty("reclaimSparkline");

        long recentHourCount = recentHourRows == null || recentHourRows.isEmpty()
                ? 0L
                : ((Number) recentHourRows.get(0).get("n")).longValue();

        exchange.getMessage().setBody(
                DashboardRenderer.reclaimActivity(recentHourCount, top5, sparkline));
    }

    private static String loadResource(String classpathResource) throws IOException {
        try (InputStream is = DashboardRoute.class.getClassLoader().getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalStateException("Missing classpath resource: " + classpathResource);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
