package io.github.leonardsibelius.orders.routes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonLibrary;

public class OrderSyncRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        String markSent = loadSql("sql/mark-order-sent.sql");
        String markFailed = loadSql("sql/mark-order-failed.sql");

        errorHandler(deadLetterChannel("kafka:orders.dlq")
                .useOriginalMessage()
                .maximumRedeliveries(3)
                .redeliveryDelay(2000));

        from("sql:classpath:sql/claim-new-orders.sql"
                + "?dataSource=#ordersDataSource"
                + "&delay=2000"
                + "&onConsume=RAW(" + markSent + ")"
                + "&onConsumeFailed=RAW(" + markFailed + ")"
                + "&useIterator=true")
            .routeId("orders-postgres-to-kafka")
            .setProperty("orderId", simple("${body[id]}"))
            .log("Polled order id=${exchangeProperty.orderId}")
            .bean("orderEnricher", "toEvent")
            .setHeader(KafkaConstants.KEY, simple("${exchangeProperty.orderId}", String.class))
            .marshal().json(JsonLibrary.Jackson)
            .to("kafka:orders.new")
            .log("Published order id=${exchangeProperty.orderId}");
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
