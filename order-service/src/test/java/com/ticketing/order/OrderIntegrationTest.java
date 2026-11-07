package com.ticketing.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.env.Environment;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import tools.jackson.databind.json.JsonMapper;

/**
 * Teste de integração do fluxo POST /orders -> banco -> Kafka, com Postgres e Kafka reais
 * em containers. O @ServiceConnection faz o Spring Boot apontar o datasource e o
 * bootstrap-servers para os containers, sobrescrevendo o que está no application.yaml.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Autowired
    Environment environment;

    @Autowired
    OrderRepository orderRepository;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void postOrder_returns202_savesPendingOrder_andPublishesOrderCreated() throws Exception {
        UUID userId = UUID.randomUUID();
        UUID showId = UUID.randomUUID();

        HttpResponse<String> response = post(orderJson(userId, showId, 2));

        assertThat(response.statusCode()).isEqualTo(202);
        Map<?, ?> body = json.readValue(response.body(), Map.class);
        assertThat(body.get("status")).isEqualTo("PENDING");
        UUID orderId = UUID.fromString((String) body.get("orderId"));

        // O pedido foi gravado no banco como PENDING
        Order saved = orderRepository.findById(orderId).orElseThrow();
        assertThat(saved.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(saved.getQuantity()).isEqualTo(2);
        assertThat(saved.getShowId()).isEqualTo(showId);

        // O evento chegou ao tópico, com o showId como chave
        ConsumerRecord<String, String> record = awaitRecordFor(orderId);
        assertThat(record.key()).isEqualTo(showId.toString());

        Map<?, ?> event = json.readValue(record.value(), Map.class);
        assertThat(event.get("orderId")).isEqualTo(orderId.toString());
        assertThat(event.get("userId")).isEqualTo(userId.toString());
        assertThat(event.get("showId")).isEqualTo(showId.toString());
        assertThat(event.get("quantity")).isEqualTo(2);
        assertThat(event.get("messageId")).isNotNull();
        assertThat(event.get("occurredAt")).isNotNull();
    }

    @Test
    void postOrder_withInvalidQuantity_returns400() throws Exception {
        HttpResponse<String> response = post(orderJson(UUID.randomUUID(), UUID.randomUUID(), 0));

        assertThat(response.statusCode()).isEqualTo(400);
    }

    @Test
    void getOrder_unknownId_returns404() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl() + "/orders/" + UUID.randomUUID()))
                .GET()
                .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
    }

    // ---------------------------------------------------------------- helpers

    private String baseUrl() {
        return "http://localhost:" + environment.getProperty("local.server.port");
    }

    private HttpResponse<String> post(String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl() + "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String orderJson(UUID userId, UUID showId, int quantity) {
        return """
                {"userId":"%s","showId":"%s","quantity":%d}
                """.formatted(userId, showId, quantity);
    }

    /** Lê o tópico orders.created desde o início até achar a mensagem do pedido (máx. 20 s). */
    private ConsumerRecord<String, String> awaitRecordFor(UUID orderId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(OrderEventPublisher.ORDERS_CREATED_TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value().contains(orderId.toString())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("OrderCreated não chegou ao tópico para orderId=" + orderId);
    }
}