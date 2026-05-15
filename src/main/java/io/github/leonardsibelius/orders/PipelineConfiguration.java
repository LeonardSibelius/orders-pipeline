package io.github.leonardsibelius.orders;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.leonardsibelius.orders.dashboard.KafkaTopicStats;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Configuration;
import org.apache.camel.PropertyInject;

@Configuration
public class PipelineConfiguration {

    @BindToRegistry("ordersDataSource")
    public DataSource ordersDataSource(
            @PropertyInject("postgres.url") String url,
            @PropertyInject("postgres.user") String user,
            @PropertyInject("postgres.password") String password,
            @PropertyInject(value = "postgres.pool.maxSize", defaultValue = "8") int maxPoolSize) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setPoolName("orders-pool");
        return new HikariDataSource(config);
    }

    @BindToRegistry("objectMapper")
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @BindToRegistry("kafkaTopicStats")
    public KafkaTopicStats kafkaTopicStats(
            @PropertyInject("camel.component.kafka.brokers") String bootstrapServers) {
        return new KafkaTopicStats(bootstrapServers);
    }
}
