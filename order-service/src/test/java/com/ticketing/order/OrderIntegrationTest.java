package com.ticketing.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
 * Teste de integração do order-service com Postgres e Kafka reais em containers.
 * Cobre a criação do pedido (POST /orders -> banco -> Kafka) e o consumo das respostas do
 * ticket-service, que aqui são simuladas publicando as mensagens direto nos tópicos.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderIntegrationTest {

    private static final String TICKETS_RESERVED = "tickets.reserved";
    private static final String TICKETS_FAILED = "tickets.reservation-failed";

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    private static KafkaProducer<String, String> producer;

    @Autowired
    Environment environment;

    @Autowired
    OrderRepository orderRepository;

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    @BeforeAll
    static void setUpKafka() throws Exception {
        // Cria os tópicos (1 partição cada) antes de a aplicação subir. Com uma só partição, a
        // ordem das mensagens de um tópico é garantida, o que os testes abaixo aproveitam.
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(OrderEventPublisher.ORDERS_CREATED_TOPIC, 1, (short) 1),
                    new NewTopic(TICKETS_RESERVED, 1, (short) 1),
                    new NewTopic(TICKETS_FAILED, 1, (short) 1))).all().get();
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @AfterAll
    static void tearDownKafka() {
        if (producer != null) {
            producer.close();
        }
    }

    // ------------------------------------------------------------ criação do pedido

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

    // ------------------------------------------------------- respostas do ticket-service

    @Test
    void ticketsReserved_confirmsPendingOrder() throws Exception {
        UUID orderId = createOrder();

        sendReserved(orderId);

        awaitStatus(orderId, OrderStatus.CONFIRMED);
    }

    @Test
    void ticketsReservationFailed_cancelsPendingOrder() throws Exception {
        UUID orderId = createOrder();

        sendFailed(orderId);

        awaitStatus(orderId, OrderStatus.CANCELLED);
    }

    @Test
    void replyForAnAlreadyFinalOrder_isIgnored() throws Exception {
        UUID confirmed = createOrder();
        sendReserved(confirmed);
        awaitStatus(confirmed, OrderStatus.CONFIRMED);

        // Resposta contraditória para um pedido já confirmado
        sendFailed(confirmed);

        // O tópico tem uma partição, então, quando este pedido for cancelado, a mensagem
        // contraditória anterior já foi processada
        UUID sentinel = createOrder();
        sendFailed(sentinel);
        awaitStatus(sentinel, OrderStatus.CANCELLED);

        assertThat(statusOf(confirmed)).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void replyForUnknownOrder_doesNotStopTheConsumer() throws Exception {
        sendFailed(UUID.randomUUID());

        UUID orderId = createOrder();
        sendFailed(orderId);

        awaitStatus(orderId, OrderStatus.CANCELLED);
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

    private UUID createOrder() throws Exception {
        HttpResponse<String> response = post(orderJson(UUID.randomUUID(), UUID.randomUUID(), 1));
        assertThat(response.statusCode()).isEqualTo(202);
        Map<?, ?> body = json.readValue(response.body(), Map.class);
        return UUID.fromString((String) body.get("orderId"));
    }

    private void sendReserved(UUID orderId) throws Exception {
        String value = """
                {"messageId":"%s","occurredAt":"%s","orderId":"%s","showId":"%s","quantity":1}
                """.formatted(UUID.randomUUID(), Instant.now(), orderId, UUID.randomUUID());
        producer.send(new ProducerRecord<>(TICKETS_RESERVED, orderId.toString(), value)).get();
    }

    private void sendFailed(UUID orderId) throws Exception {
        String value = """
                {"messageId":"%s","occurredAt":"%s","orderId":"%s","showId":"%s","reason":"OUT_OF_STOCK"}
                """.formatted(UUID.randomUUID(), Instant.now(), orderId, UUID.randomUUID());
        producer.send(new ProducerRecord<>(TICKETS_FAILED, orderId.toString(), value)).get();
    }

    private OrderStatus statusOf(UUID orderId) {
        return orderRepository.findById(orderId).orElseThrow().getStatus();
    }

    /** Espera, por até 20 s, o pedido chegar ao estado esperado. */
    private void awaitStatus(UUID orderId, OrderStatus expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        OrderStatus current = null;
        while (System.nanoTime() < deadline) {
            current = orderRepository.findById(orderId).map(Order::getStatus).orElse(null);
            if (current == expected) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Pedido " + orderId + " deveria estar " + expected + ", mas está " + current);
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