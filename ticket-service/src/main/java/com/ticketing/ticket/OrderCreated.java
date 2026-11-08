package com.ticketing.ticket;

import java.time.Instant;
import java.util.UUID;

/**
 * Cópia local do contrato do evento publicado pelo order-service em "orders.created".
 * Os serviços não compartilham classes: o contrato é o JSON.
 */
public record OrderCreated(
        UUID messageId,
        Instant occurredAt,
        UUID orderId,
        UUID userId,
        UUID showId,
        int quantity) {
}
