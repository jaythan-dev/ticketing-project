package com.ticketing.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    public record CreateOrderRequest(
            @NotNull UUID userId,
            @NotNull UUID showId,
            @Min(1) @Max(10) int quantity) {
    }

    public record OrderResponse(
            UUID orderId,
            UUID userId,
            UUID showId,
            int quantity,
            OrderStatus status,
            Instant createdAt) {

        static OrderResponse from(Order order) {
            return new OrderResponse(
                    order.getId(),
                    order.getUserId(),
                    order.getShowId(),
                    order.getQuantity(),
                    order.getStatus(),
                    order.getCreatedAt());
        }
    }

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    // 202 Accepted: o pedido foi aceito, mas o resultado final (CONFIRMED ou CANCELLED)
    // só é conhecido depois que o ticket-service responder, de forma assíncrona.
    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        Order order = service.createOrder(request.userId(), request.showId(), request.quantity());
        return ResponseEntity
                .accepted()
                .location(URI.create("/orders/" + order.getId()))
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
        return service.findById(id)
                .map(order -> ResponseEntity.ok(OrderResponse.from(order)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}