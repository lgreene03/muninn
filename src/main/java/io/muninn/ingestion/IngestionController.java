package io.muninn.ingestion;

import io.muninn.event.Event;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/events")
public class IngestionController {

    private final EventProducer eventProducer;

    public IngestionController(EventProducer eventProducer) {
        this.eventProducer = eventProducer;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody @Valid Event event) {
        eventProducer.publish(event.eventType(), event);
        return ResponseEntity
                .created(URI.create("/api/v1/events/" + event.eventId()))
                .body(Map.of("eventId", event.eventId(), "status", "accepted"));
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> ingestBatch(@RequestBody List<@Valid Event> events) {
        events.forEach(event -> eventProducer.publish(event.eventType(), event));
        return ResponseEntity
                .accepted()
                .body(Map.of("accepted", events.size()));
    }
}
