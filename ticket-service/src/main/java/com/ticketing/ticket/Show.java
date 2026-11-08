package com.ticketing.ticket;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "shows")
public class Show {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(nullable = false)
    private int capacity;

    @Column(nullable = false)
    private int available;

    protected Show() {
        // exigido pelo JPA
    }

    public Show(String name, int capacity) {
        this.name = name;
        this.capacity = capacity;
        this.available = capacity;
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public int getCapacity() { return capacity; }
    public int getAvailable() { return available; }
}
