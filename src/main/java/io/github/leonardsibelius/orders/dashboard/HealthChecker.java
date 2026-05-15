package io.github.leonardsibelius.orders.dashboard;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.camel.CamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reachability probes for the three things the dashboard cares about:
 * the Postgres data source, the Kafka broker, and the Camel context itself.
 * Returns a LinkedHashMap so the dashboard renders them in a stable order.
 *
 * <p>Each probe is bounded in time (HikariCP enforces connection-timeout
 * on getConnection; KafkaTopicStats applies a 2s admin timeout) so a single
 * unhealthy dependency cannot stall the /api/health endpoint.
 *
 * <p>Constructed via {@code PipelineConfiguration#healthChecker(...)} so
 * its dependencies are resolved in factory-method order rather than via
 * post-construction field injection (which races with classpath scan order).
 */
public class HealthChecker {

    private static final Logger LOG = LoggerFactory.getLogger(HealthChecker.class);

    private final DataSource dataSource;
    private final KafkaTopicStats kafkaTopicStats;
    private final CamelContext camelContext;

    public HealthChecker(DataSource dataSource, KafkaTopicStats kafkaTopicStats, CamelContext camelContext) {
        this.dataSource = dataSource;
        this.kafkaTopicStats = kafkaTopicStats;
        this.camelContext = camelContext;
    }

    public Map<String, Boolean> check() {
        Map<String, Boolean> m = new LinkedHashMap<>();
        m.put("postgres", pingPostgres());
        m.put("kafka", kafkaTopicStats.isReachable());
        m.put("camel", camelOk());
        return m;
    }

    private boolean pingPostgres() {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet r = s.executeQuery("SELECT 1")) {
            return r.next();
        } catch (SQLException e) {
            LOG.debug("Postgres ping failed: {}", e.getMessage());
            return false;
        }
    }

    private boolean camelOk() {
        if (!camelContext.getStatus().isStarted()) {
            return false;
        }
        var rc = camelContext.getRouteController();
        return camelContext.getRoutes().stream()
                .allMatch(r -> rc.getRouteStatus(r.getId()).isStarted());
    }
}
