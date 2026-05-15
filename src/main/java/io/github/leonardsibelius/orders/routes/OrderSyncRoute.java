package io.github.leonardsibelius.orders.routes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;

public class OrderSyncRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        String markSent = loadSql("sql/mark-order-sent.sql");
        String markFailed = loadSql("sql/mark-order-failed.sql");

        // Marshal with the registry ObjectMapper from PipelineConfiguration,
        // which registers JavaTimeModule -- required for OrderEvent's Instant
        // fields. The fluent .marshal().json(...) form would build its own
        // ObjectMapper without that module and fail to serialize.
        JsonDataFormat orderEventJson = new JsonDataFormat(JsonLibrary.Jackson);
        orderEventJson.setObjectMapper("objectMapper");

        errorHandler(deadLetterChannel("kafka:orders.dlq")
                .maximumRedeliveries(3)
                .redeliveryDelay(2000));

        from("sql:classpath:sql/claim-new-orders.sql"
                + "?dataSource=#ordersDataSource"
                + "&delay=2000"
                + "&onConsume=RAW(" + markSent + ")"
                + "&onConsumeFailed=RAW(" + markFailed + ")"
                + "&useIterator=true")
            .routeId("orders-postgres-to-kafka")
            .setHeader("orderId", simple("${body['id']}"))
            .log("Polled order id=${header.orderId}")
            .bean("orderEnricher", "toEvent")
            .setHeader(KafkaConstants.KEY, simple("${header.orderId}", String.class))
            .marshal(orderEventJson)
            .to("kafka:orders.new")
            .log("Published order id=${header.orderId}");
    }

    private static String loadSql(String classpathResource) throws IOException {
        try (InputStream is = OrderSyncRoute.class.getClassLoader().getResourceAsStream(classpathResource)) {
            if (is == null) {
                throw new IllegalStateException("Missing SQL resource: " + classpathResource);
            }
            return new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
    }
}
