package com.ticketing.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consome as respostas do ticket-service e leva o pedido de PENDING para
 * CONFIRMED ou CANCELLED.
 */
@Component
public class TicketReplyListener {

    private static final Logger log = LoggerFactory.getLogger(TicketReplyListener.class);

    private final OrderService orderService;

    public TicketReplyListener(OrderService orderService) {
        this.orderService = orderService;
    }

    // O tipo do payload é definido por listener, porque cada tópico tem um evento diferente
    @KafkaListener(
            topics = "tickets.reserved",
            properties = "spring.json.value.default.type=com.ticketing.order.TicketsReserved")
    public void onTicketsReserved(TicketsReserved event) {
        log.info("TicketsReserved recebido orderId={}", event.orderId());
        orderService.confirm(event.orderId());
    }

    @KafkaListener(
            topics = "tickets.reservation-failed",
            properties = "spring.json.value.default.type=com.ticketing.order.TicketsReservationFailed")
    public void onTicketsReservationFailed(TicketsReservationFailed event) {
        log.info("TicketsReservationFailed recebido orderId={} reason={}", event.orderId(), event.reason());
        orderService.cancel(event.orderId());
    }
}