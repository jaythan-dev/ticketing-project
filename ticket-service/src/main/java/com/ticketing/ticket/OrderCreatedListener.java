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

    // Ainda sem idempotência: se a mesma mensagem for entregue duas vezes, reserva duas vezes.
    // O messageId do evento será usado para resolver isso no próximo passo.
    @KafkaListener(topics = "orders.created")
    public void onOrderCreated(OrderCreated event) {
        log.info("OrderCreated recebido orderId={} showId={} quantity={}",
                event.orderId(), event.showId(), event.quantity());

        ReservationService.Result result = reservationService.reserve(event.showId(), event.quantity());

        if (result == ReservationService.Result.RESERVED) {
            publisher.publishReserved(TicketsReserved.from(event));
        } else {
            publisher.publishFailed(TicketsReservationFailed.from(event, result.name()));
        }
    }
}
