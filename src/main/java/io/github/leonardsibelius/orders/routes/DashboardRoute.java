package io.github.leonardsibelius.orders.routes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    }

    private static void renderStats(Exchange exchange) {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rows = exchange.getMessage().getBody(List.class);
        Map<String, Long> counts = rows == null ? Map.of() : rows.stream().collect(Collectors.toMap(
                r -> String.valueOf(r.get("status")),
                r -> ((Number) r.get("n")).longValue()));

        String html = """
                <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-blue-400">New</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1">%d</div>
                  </div>
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-amber-400">In progress</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1">%d</div>
                  </div>
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-emerald-400">Sent</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1">%d</div>
                  </div>
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-rose-400">Error</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1">%d</div>
                  </div>
                </div>
                """.formatted(
                        counts.getOrDefault("NEW", 0L),
                        counts.getOrDefault("IN_PROGRESS", 0L),
                        counts.getOrDefault("SENT", 0L),
                        counts.getOrDefault("ERROR", 0L));

        exchange.getMessage().setBody(html);
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
