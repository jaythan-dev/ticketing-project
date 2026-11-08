package com.ticketing.ticket;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/shows")
public class ShowController {

    public record CreateShowRequest(
            @NotBlank String name,
            @Min(1) int capacity) {
    }

    public record ShowResponse(UUID id, String name, int capacity, int available) {

        static ShowResponse from(Show show) {
            return new ShowResponse(show.getId(), show.getName(), show.getCapacity(), show.getAvailable());
        }
    }

    private final ShowRepository repository;

    public ShowController(ShowRepository repository) {
        this.repository = repository;
    }

    @PostMapping
    public ResponseEntity<ShowResponse> create(@Valid @RequestBody CreateShowRequest request) {
        Show show = repository.save(new Show(request.name(), request.capacity()));
        return ResponseEntity
                .created(URI.create("/shows/" + show.getId()))
                .body(ShowResponse.from(show));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ShowResponse> get(@PathVariable UUID id) {
        return repository.findById(id)
                .map(show -> ResponseEntity.ok(ShowResponse.from(show)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
