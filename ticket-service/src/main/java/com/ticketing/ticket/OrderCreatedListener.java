package com.ticketing.ticket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final ReservationService reservationService;
    private final TicketEventPublisher publisher;

    public OrderCreatedListener(ReservationService reservationService, TicketEventPublisher publisher) {
        this.reservationService = reservationService;
        this.publisher = publisher;
    }

    @KafkaListener(topics = "orders.created")
    public void onOrderCreated(OrderCreated event) {
        if (event.messageId() == null) {
            log.error("OrderCreated sem messageId, descartado orderId={}", event.orderId());
            return;
        }

        log.info("OrderCreated recebido messageId={} orderId={} showId={} quantity={}",
                event.messageId(), event.orderId(), event.showId(), event.quantity());

        ReservationService.Result result =
                reservationService.reserve(event.messageId(), event.showId(), event.quantity());

        // Numa entrega repetida a resposta é publicada de novo, com o mesmo resultado da primeira.
        // Isso é inofensivo para o order-service (só um pedido PENDING muda de estado) e cobre a
        // queda entre gravar a reserva e publicar a resposta.
        if (result == ReservationService.Result.RESERVED) {
            publisher.publishReserved(TicketsReserved.from(event));
        } else {
            publisher.publishFailed(TicketsReservationFailed.from(event, result.name()));
        }
    }
}