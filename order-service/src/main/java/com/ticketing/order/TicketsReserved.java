package com.ticketing.order;

import java.time.Instant;
import java.util.UUID;

/**
 * Cópia local do contrato do evento publicado pelo ticket-service em "tickets.reserved".
 * Os serviços não compartilham classes: o contrato é o JSON.
 */
public record TicketsReserved(
        UUID messageId,
        Instant occurredAt,
        UUID orderId,
        UUID showId,
        int quantity) {
}