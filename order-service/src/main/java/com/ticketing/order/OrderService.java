package com.ticketing.order;

import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private final OrderRepository repository;
    private final OrderEventPublisher publisher;

    public OrderService(OrderRepository repository, OrderEventPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    // Sem @Transactional de propósito: o save() confirma a transação sozinho e só
    // depois o evento é publicado. Se a aplicação cair entre as duas etapas, o pedido
    // fica PENDING para sempre. É esse problema que o outbox pattern vai resolver.
    public Order createOrder(UUID userId, UUID showId, int quantity) {
        Order order = repository.save(new Order(userId, showId, quantity));
        publisher.publishOrderCreated(order);
        return order;
    }

    public Optional<Order> findById(UUID id) {
        return repository.findById(id);
    }
}