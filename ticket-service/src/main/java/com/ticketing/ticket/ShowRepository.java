package com.ticketing.ticket;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface ShowRepository extends JpaRepository<Show, UUID> {

    /**
     * Reserva ingressos de forma atômica: o UPDATE só altera a linha se ainda houver
     * estoque suficiente. Retorna 1 se reservou e 0 se não havia estoque (ou o show
     * não existe). Não há "ler, checar, gravar" na aplicação, então duas reservas
     * simultâneas nunca deixam o estoque negativo.
     */
    @Modifying
    @Query("""
            update Show s
               set s.available = s.available - :quantity
             where s.id = :id
               and s.available >= :quantity
            """)
    int reserve(@Param("id") UUID id, @Param("quantity") int quantity);
}
