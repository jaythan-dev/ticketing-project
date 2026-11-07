package com.ticketing.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    public static final String ORDERS_CREATED_TOPIC = "orders.created";

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(Order order) {
        OrderCreated event = OrderCreated.from(order);

        // A chave é o showId: todos os pedidos do mesmo show vão para a mesma
        // partição e são processados em ordem pelo mesmo consumer.
        kafkaTemplate.send(ORDERS_CREATED_TOPIC, event.showId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Falha ao publicar OrderCreated orderId={}", event.orderId(), ex);
                    } else {
                        log.info("OrderCreated publicado orderId={} partition={} offset={}",
                                event.orderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}