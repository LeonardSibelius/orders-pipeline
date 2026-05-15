package io.github.leonardsibelius.orders.dashboard;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pure HTML-fragment rendering for the dashboard. No Camel types in the
 * signatures -- the route layer extracts typed inputs from the exchange,
 * calls these, and writes the result back to the body. Keeps view logic
 * out of the routes and makes these trivial to unit-test in isolation.
 */
public final class DashboardRenderer {

    private DashboardRenderer() {
    }

    public static String stats(List<Map<String, Object>> rows) {
        Map<String, Long> counts = rows == null ? Map.of() : rows.stream().collect(Collectors.toMap(
                r -> String.valueOf(r.get("status")),
                r -> ((Number) r.get("n")).longValue()));
        return """
                <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-blue-400">New</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1 tabular-nums">%d</div>
                  </div>
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-amber-400">In progress</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1 tabular-nums">%d</div>
                  </div>
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-emerald-400">Sent</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1 tabular-nums">%d</div>
                  </div>
                  <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4">
                    <div class="text-xs uppercase tracking-wider text-rose-400">Error</div>
                    <div class="text-3xl font-semibold text-slate-100 mt-1 tabular-nums">%d</div>
                  </div>
                </div>
                """.formatted(
                        counts.getOrDefault("NEW", 0L),
                        counts.getOrDefault("IN_PROGRESS", 0L),
                        counts.getOrDefault("SENT", 0L),
                        counts.getOrDefault("ERROR", 0L));
    }

    public static String recent(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return """
                    <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4 text-slate-500 text-sm">No orders yet.</div>
                    """;
        }
        StringBuilder body = new StringBuilder();
        for (Map<String, Object> r : rows) {
            body.append(renderRecentRow(r));
        }
        return """
                <div class="rounded-lg border border-slate-700 bg-slate-800/60 overflow-hidden">
                  <table class="w-full text-sm">
                    <thead class="bg-slate-900/60 text-slate-400 text-xs uppercase tracking-wider">
                      <tr>
                        <th class="text-left px-3 py-2 font-medium">id</th>
                        <th class="text-left px-3 py-2 font-medium">customer</th>
                        <th class="text-right px-3 py-2 font-medium">amount</th>
                        <th class="text-left px-3 py-2 font-medium">status</th>
                        <th class="text-right px-3 py-2 font-medium">journey</th>
                      </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-800">
                %s    </tbody>
                  </table>
                </div>
                """.formatted(body);
    }

    private static String renderRecentRow(Map<String, Object> r) {
        long id = ((Number) r.get("id")).longValue();
        String customer = String.valueOf(r.get("customer_id"));
        BigDecimal amount = (BigDecimal) r.get("amount");
        String currency = String.valueOf(r.get("currency"));
        String status = String.valueOf(r.get("status"));
        Instant createdAt = toInstant(r.get("created_at"));
        Instant claimedAt = toInstant(r.get("claimed_at"));
        Instant sentAt = toInstant(r.get("sent_at"));
        Instant erroredAt = toInstant(r.get("errored_at"));

        return """
                      <tr>
                        <td class="px-3 py-2 text-slate-300 tabular-nums">#%d</td>
                        <td class="px-3 py-2 text-slate-300">%s</td>
                        <td class="px-3 py-2 text-right text-slate-300 tabular-nums">%s %s</td>
                        <td class="px-3 py-2">%s</td>
                        <td class="px-3 py-2 text-right text-slate-400 tabular-nums">%s</td>
                      </tr>
                """.formatted(
                        id,
                        escape(customer),
                        amount == null ? "—" : amount.toPlainString(),
                        escape(currency),
                        statusBadge(status),
                        escape(formatJourney(status, createdAt, claimedAt, sentAt, erroredAt)));
    }

    public static String dlqSize(long count) {
        String tone = count == 0 ? "text-emerald-400" : "text-rose-400";
        return """
                <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-4 flex items-baseline justify-between">
                  <div>
                    <div class="text-xs uppercase tracking-wider text-slate-400">orders.dlq</div>
                    <div class="text-2xl font-semibold %s mt-1 tabular-nums">%d</div>
                  </div>
                  <div class="text-xs text-slate-500">messages</div>
                </div>
                """.formatted(tone, count);
    }

    public static String stuck(List<Map<String, Object>> rows) {
        long count = rows == null || rows.isEmpty() ? 0
                : ((Number) rows.get(0).get("n")).longValue();
        if (count == 0) {
            return """
                    <div class="rounded-lg border border-slate-700 bg-slate-800/60 p-3 text-slate-500 text-xs">No rows stuck in IN_PROGRESS &gt; 5m.</div>
                    """;
        }
        return """
                <div class="rounded-lg border border-rose-700/50 bg-rose-950/40 p-4">
                  <div class="text-rose-200 font-medium">⚠ %d row(s) stuck in IN_PROGRESS for more than 5 minutes</div>
                  <div class="text-xs text-rose-300/70 mt-2">The v1.2 reaper will recover these automatically. To unstick manually: <code class="text-rose-200">UPDATE orders SET status='NEW', claimed_at=NULL WHERE status='IN_PROGRESS' AND claimed_at &lt; NOW() - INTERVAL '5 minutes'</code></div>
                </div>
                """.formatted(count);
    }

    // ---------------------- helpers ----------------------

    private static String statusBadge(String status) {
        String classes = switch (status) {
            case "NEW" -> "bg-blue-500/10 text-blue-300 border border-blue-500/20";
            case "IN_PROGRESS" -> "bg-amber-500/10 text-amber-300 border border-amber-500/20";
            case "SENT" -> "bg-emerald-500/10 text-emerald-300 border border-emerald-500/20";
            case "ERROR" -> "bg-rose-500/10 text-rose-300 border border-rose-500/20";
            default -> "bg-slate-700/40 text-slate-300";
        };
        return "<span class=\"inline-block px-2 py-0.5 rounded text-xs font-medium "
                + classes + "\">" + escape(status) + "</span>";
    }

    private static String formatJourney(String status, Instant createdAt, Instant claimedAt,
                                        Instant sentAt, Instant erroredAt) {
        Instant now = Instant.now();
        return switch (status) {
            case "NEW" -> "queued " + ago(now, createdAt);
            case "IN_PROGRESS" -> "in-flight " + ago(now, claimedAt != null ? claimedAt : createdAt);
            case "SENT" -> sentAt != null ? duration(createdAt, sentAt) + " ✓" : "—";
            case "ERROR" -> erroredAt != null ? duration(createdAt, erroredAt) + " ✗" : "—";
            default -> "—";
        };
    }

    private static String ago(Instant now, Instant t) {
        return t == null ? "" : duration(t, now) + " ago";
    }

    private static String duration(Instant a, Instant b) {
        if (a == null || b == null) return "—";
        long ms = Duration.between(a, b).toMillis();
        if (ms < 0) return "0ms";
        if (ms < 1000) return ms + "ms";
        if (ms < 60_000) return String.format("%.1fs", ms / 1000.0);
        if (ms < 3_600_000) return (ms / 60_000) + "m " + ((ms / 1000) % 60) + "s";
        return (ms / 3_600_000) + "h " + ((ms / 60_000) % 60) + "m";
    }

    private static Instant toInstant(Object value) {
        if (value == null) return null;
        if (value instanceof Instant i) return i;
        if (value instanceof OffsetDateTime odt) return odt.toInstant();
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant();
        if (value instanceof LocalDateTime ldt) return ldt.toInstant(ZoneOffset.UTC);
        return null;
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
