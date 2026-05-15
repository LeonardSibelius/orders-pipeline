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

    private static String loadResource(String classpathResource) throws IOException {
        try (InputStream is = DashboardRoute.class.getClassLoader().getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalStateException("Missing classpath resource: " + classpathResource);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
