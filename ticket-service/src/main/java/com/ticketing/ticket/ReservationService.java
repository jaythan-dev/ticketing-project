package com.ticketing.ticket;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class ReservationService {

    public enum Result {
        RESERVED,
        OUT_OF_STOCK,
        SHOW_NOT_FOUND
    }

    private final ShowRepository showRepository;

    public ReservationService(ShowRepository showRepository) {
        this.showRepository = showRepository;
    }

    @Transactional
    public Result reserve(UUID showId, int quantity) {
        if (showRepository.reserve(showId, quantity) == 1) {
            return Result.RESERVED;
        }
        return showRepository.existsById(showId) ? Result.OUT_OF_STOCK : Result.SHOW_NOT_FOUND;
    }
}
