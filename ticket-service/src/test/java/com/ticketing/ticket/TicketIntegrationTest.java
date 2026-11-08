package com.ticketing.ticket;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import tools.jackson.databind.json.JsonMapper;

/**
 * Publica OrderCreated direto no Kafka (como o order-service faria) e verifica a resposta
 * do ticket-service e o estoque no banco, com Postgres e Kafka reais em containers.
 */
@SpringBootTest
@Testcontainers
class TicketIntegrationTest {

    private static final String ORDERS_CREATED = "orders.created";
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
    ShowRepository showRepository;

    private final JsonMapper json = JsonMapper.builder().build();

    private record Reply(String topic, Map<?, ?> body) {
    }

    @BeforeAll
    static void setUpKafka() throws Exception {
        // Cria os tópicos antes de a aplicação subir, para o consumer já encontrá-los
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(ORDERS_CREATED, 1, (short) 1),
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

    @Test
    void orderCreated_withStock_reservesTicketsAndPublishesTicketsReserved() throws Exception {
        Show show = showRepository.save(new Show("Show A", 10));
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, show.getId(), 2);

        Reply reply = awaitReplies(Set.of(orderId)).get(orderId);
        assertThat(reply.topic()).isEqualTo(TICKETS_RESERVED);
        assertThat(reply.body().get("orderId")).isEqualTo(orderId.toString());
        assertThat(reply.body().get("quantity")).isEqualTo(2);

        assertThat(showRepository.findById(show.getId()).orElseThrow().getAvailable()).isEqualTo(8);
    }

    @Test
    void orderCreated_withoutEnoughStock_publishesFailureAndKeepsStock() throws Exception {
        Show show = showRepository.save(new Show("Show B", 1));
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, show.getId(), 2);

        Reply reply = awaitReplies(Set.of(orderId)).get(orderId);
        assertThat(reply.topic()).isEqualTo(TICKETS_FAILED);
        assertThat(reply.body().get("reason")).isEqualTo("OUT_OF_STOCK");

        assertThat(showRepository.findById(show.getId()).orElseThrow().getAvailable()).isEqualTo(1);
    }

    @Test
    void orderCreated_forUnknownShow_publishesShowNotFound() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendOrderCreated(orderId, UUID.randomUUID(), 1);

        Reply reply = awaitReplies(Set.of(orderId)).get(orderId);
        assertThat(reply.topic()).isEqualTo(TICKETS_FAILED);
        assertThat(reply.body().get("reason")).isEqualTo("SHOW_NOT_FOUND");
    }

    @Test
    void manyOrdersForLimitedStock_neverOversell() throws Exception {
        Show show = showRepository.save(new Show("Show C", 5));
        Set<UUID> orderIds = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            UUID orderId = UUID.randomUUID();
            orderIds.add(orderId);
            sendOrderCreated(orderId, show.getId(), 1);
        }

        Map<UUID, Reply> replies = awaitReplies(orderIds);

        long reserved = replies.values().stream().filter(r -> r.topic().equals(TICKETS_RESERVED)).count();
        long failed = replies.values().stream().filter(r -> r.topic().equals(TICKETS_FAILED)).count();
        assertThat(reserved).isEqualTo(5);
        assertThat(failed).isEqualTo(15);
        assertThat(showRepository.findById(show.getId()).orElseThrow().getAvailable()).isZero();
    }

    // ---------------------------------------------------------------- helpers

    private void sendOrderCreated(UUID orderId, UUID showId, int quantity) throws Exception {
        String value = """
                {"messageId":"%s","occurredAt":"%s","orderId":"%s","userId":"%s","showId":"%s","quantity":%d}
                """.formatted(UUID.randomUUID(), Instant.now(), orderId, UUID.randomUUID(), showId, quantity);
        producer.send(new ProducerRecord<>(ORDERS_CREATED, showId.toString(), value)).get();
    }

    /** Lê as respostas (reservado ou falha) dos pedidos informados, por até 30 s. */
    private Map<UUID, Reply> awaitReplies(Set<UUID> orderIds) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Map<UUID, Reply> replies = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TICKETS_RESERVED, TICKETS_FAILED));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (replies.size() < orderIds.size() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    UUID orderId = UUID.fromString(record.key());
                    if (orderIds.contains(orderId)) {
                        replies.put(orderId, new Reply(record.topic(), json.readValue(record.value(), Map.class)));
                    }
                }
            }
        }
        assertThat(replies).as("respostas recebidas para os pedidos").hasSize(orderIds.size());
        return replies;
    }
}
