package com.ticketing.order;

import java.time.Instant;
import java.util.UUID;

/**
 * Evento publicado em "orders.created".
 * messageId será a base da idempotência no consumer do ticket-service.
 */
public record OrderCreated(
        UUID messageId,
        Instant occurredAt,
        UUID orderId,
        UUID userId,
        UUID showId,
        int quantity) {

    public static OrderCreated from(Order order) {
        return new OrderCreated(
                UUID.randomUUID(),
                Instant.now(),
                order.getId(),
                order.getUserId(),
                order.getShowId(),
                order.getQuantity());
    }
}