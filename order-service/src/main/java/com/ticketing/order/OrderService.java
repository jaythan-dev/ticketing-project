package com.ticketing.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

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

    @Transactional
    public void confirm(UUID orderId) {
        transition(orderId, OrderStatus.CONFIRMED);
    }

    @Transactional
    public void cancel(UUID orderId) {
        transition(orderId, OrderStatus.CANCELLED);
    }

    /**
     * Só um pedido PENDING muda de estado. Uma resposta repetida ou contraditória para um
     * pedido já finalizado é ignorada, o que também torna a reentrega da mesma mensagem
     * inofensiva neste serviço.
     */
    private void transition(UUID orderId, OrderStatus target) {
        Optional<Order> found = repository.findById(orderId);
        if (found.isEmpty()) {
            log.warn("Resposta para pedido inexistente orderId={} (destino {})", orderId, target);
            return;
        }

        Order order = found.get();
        if (order.getStatus() != OrderStatus.PENDING) {
            log.info("Pedido orderId={} já está {}; resposta para {} ignorada",
                    orderId, order.getStatus(), target);
            return;
        }

        if (target == OrderStatus.CONFIRMED) {
            order.confirm();
        } else {
            order.cancel();
        }
        log.info("Pedido orderId={} agora está {}", orderId, target);
    }
}