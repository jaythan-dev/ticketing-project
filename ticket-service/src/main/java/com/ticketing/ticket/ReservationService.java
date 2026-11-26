package com.ticketing.ticket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
public class ReservationService {

    public enum Result {
        RESERVED,
        OUT_OF_STOCK,
        SHOW_NOT_FOUND
    }

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final ShowRepository showRepository;
    private final ProcessedMessageRepository processedMessageRepository;

    public ReservationService(ShowRepository showRepository,
                              ProcessedMessageRepository processedMessageRepository) {
        this.showRepository = showRepository;
        this.processedMessageRepository = processedMessageRepository;
    }

    /**
     * Reserva os ingressos de forma idempotente. A reserva e o registro do messageId acontecem
     * na mesma transação: ou os dois são gravados, ou nenhum.
     *
     * Se a mesma mensagem chegar de novo, o estoque não muda e o resultado original é devolvido,
     * para que a resposta possa ser reenviada. Se duas entregas da mesma mensagem disputarem ao
     * mesmo tempo, a chave primária de processed_messages faz a segunda transação falhar e desfazer
     * a reserva; na nova tentativa, ela encontra o registro da primeira.
     */
    @Transactional
    public Result reserve(UUID messageId, UUID showId, int quantity) {
        Optional<ProcessedMessage> previous = processedMessageRepository.findById(messageId);
        if (previous.isPresent()) {
            log.info("Mensagem {} já processada ({}); estoque não alterado", messageId, previous.get().getResult());
            return Result.valueOf(previous.get().getResult());
        }

        Result result = doReserve(showId, quantity);
        processedMessageRepository.saveAndFlush(new ProcessedMessage(messageId, result.name()));
        return result;
    }

    private Result doReserve(UUID showId, int quantity) {
        if (showRepository.reserve(showId, quantity) == 1) {
            return Result.RESERVED;
        }
        return showRepository.existsById(showId) ? Result.OUT_OF_STOCK : Result.SHOW_NOT_FOUND;
    }
}