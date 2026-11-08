package com.ticketing.ticket;

import java.time.Instant;
import java.util.UUID;

/** Evento publicado em "tickets.reservation-failed". */
public record TicketsReservationFailed(
        UUID messageId,
        Instant occurredAt,
        UUID orderId,
        UUID showId,
        String reason) {

    public static TicketsReservationFailed from(OrderCreated order, String reason) {
        return new TicketsReservationFailed(
                UUID.randomUUID(), Instant.now(), order.orderId(), order.showId(), reason);
    }
}
