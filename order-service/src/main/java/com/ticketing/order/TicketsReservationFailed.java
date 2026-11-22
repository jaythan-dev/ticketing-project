package com.ticketing.order;

import java.time.Instant;
import java.util.UUID;

/**
 * Cópia local do contrato do evento publicado pelo ticket-service em
 * "tickets.reservation-failed". O motivo é, por exemplo, OUT_OF_STOCK ou SHOW_NOT_FOUND.
 */
public record TicketsReservationFailed(
        UUID messageId,
        Instant occurredAt,
        UUID orderId,
        UUID showId,
        String reason) {
}