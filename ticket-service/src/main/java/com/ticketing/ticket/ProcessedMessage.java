package com.ticketing.ticket;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/** Mensagem de orders.created já processada, com o resultado que foi dado a ela. */
@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    @Column(name = "message_id")
    private UUID messageId;

    @Column(nullable = false, length = 30)
    private String result;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected ProcessedMessage() {
        // exigido pelo JPA
    }

    public ProcessedMessage(UUID messageId, String result) {
        this.messageId = messageId;
        this.result = result;
        this.processedAt = Instant.now();
    }

    public UUID getMessageId() { return messageId; }
    public String getResult() { return result; }
    public Instant getProcessedAt() { return processedAt; }
}