package com.ticketing.ticket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TicketEventPublisher {

    public static final String TICKETS_RESERVED_TOPIC = "tickets.reserved";
    public static final String TICKETS_RESERVATION_FAILED_TOPIC = "tickets.reservation-failed";

    private static final Logger log = LoggerFactory.getLogger(TicketEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TicketEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishReserved(TicketsReserved event) {
        send(TICKETS_RESERVED_TOPIC, event.orderId().toString(), event);
    }

    public void publishFailed(TicketsReservationFailed event) {
        send(TICKETS_RESERVATION_FAILED_TOPIC, event.orderId().toString(), event);
    }

    // A chave é o orderId: as respostas de um mesmo pedido ficam na mesma partição.
    private void send(String topic, String key, Object event) {
        kafkaTemplate.send(topic, key, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Falha ao publicar em {} (orderId={})", topic, key, ex);
            } else {
                log.info("Publicado em {} orderId={} partition={} offset={}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}
