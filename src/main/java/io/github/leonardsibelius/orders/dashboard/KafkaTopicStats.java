package io.github.leonardsibelius.orders.dashboard;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Long-lived Kafka AdminClient wrapper for the dashboard. Returns 0 on any
 * failure (topic absent, broker unreachable, timeout) so the dashboard
 * always renders cleanly; the dedicated system-health panel will surface
 * the actual broker reachability separately.
 */
public class KafkaTopicStats implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicStats.class);
    private static final long TIMEOUT_MS = 2000;

    private final AdminClient admin;

    public KafkaTopicStats(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_MS));
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_MS));
        this.admin = AdminClient.create(props);
    }

    public long messagesIn(String topic) {
        try {
            var partitions = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .get(topic)
                    .partitions();

            Map<TopicPartition, OffsetSpec> earliestQuery = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latestQuery = new HashMap<>();
            for (var p : partitions) {
                TopicPartition tp = new TopicPartition(topic, p.partition());
                earliestQuery.put(tp, OffsetSpec.earliest());
                latestQuery.put(tp, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResultInfo> earliest =
                    admin.listOffsets(earliestQuery).all().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Map<TopicPartition, ListOffsetsResultInfo> latest =
                    admin.listOffsets(latestQuery).all().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            long total = 0;
            for (var tp : earliest.keySet()) {
                total += latest.get(tp).offset() - earliest.get(tp).offset();
            }
            return total;
        } catch (Exception e) {
            LOG.debug("Could not read message count for topic {}: {}", topic, e.getMessage());
            return 0;
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
