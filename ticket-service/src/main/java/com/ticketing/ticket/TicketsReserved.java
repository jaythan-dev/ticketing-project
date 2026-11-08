package com.ticketing.ticket;

import java.time.Instant;
import java.util.UUID;

/** Evento publicado em "tickets.reserved". */
public record TicketsReserved(
        UUID messageId,
        Instant occurredAt,
        UUID orderId,
        UUID showId,
        int quantity) {

    public static TicketsReserved from(OrderCreated order) {
        return new TicketsReserved(
                UUID.randomUUID(), Instant.now(), order.orderId(), order.showId(), order.quantity());
    }
}
