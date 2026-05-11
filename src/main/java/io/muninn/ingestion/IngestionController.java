package io.muninn.ingestion;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.validation.EventValidator;
import io.muninn.shared.validation.ValidationResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Map;

/**
 * HTTP endpoint for synthetic or test event injection.
 *
 * <p>This controller exists for testing and smoke-test purposes. Production ingestion
 * flows through the WebSocket adapter, not this endpoint. Accepts typed
 * {@link MarketEvent} subtypes (via JSON with a type discriminator) and routes them
 * through the same validation → publish pipeline as the adapter path.</p>
 */
@RestController
@RequestMapping("/api/v1/events")
public class IngestionController {

    private final MarketEventProducer eventProducer;
    private final DeadLetterProducer deadLetterProducer;
    private final EventValidator eventValidator;

    public IngestionController(MarketEventProducer eventProducer,
                               DeadLetterProducer deadLetterProducer) {
        this.eventProducer = eventProducer;
        this.deadLetterProducer = deadLetterProducer;
        this.eventValidator = new EventValidator();
    }

    @PostMapping("/trade")
    public ResponseEntity<Map<String, Object>> ingestTrade(@RequestBody TradeEvent event) {
        return processEvent(event);
    }

    @PostMapping("/book-snapshot")
    public ResponseEntity<Map<String, Object>> ingestBookSnapshot(@RequestBody OrderBookSnapshotEvent event) {
        return processEvent(event);
    }

    private ResponseEntity<Map<String, Object>> processEvent(MarketEvent event) {
        ValidationResult result = eventValidator.validate(event);

        return switch (result) {
            case ValidationResult.Valid valid -> {
                eventProducer.publish(event);
                yield ResponseEntity
                        .created(URI.create("/api/v1/events/" + event.eventId()))
                        .body(Map.of(
                                "eventId", event.eventId(),
                                "status", "accepted",
                                "topic", event.topicName()
                        ));
            }
            case ValidationResult.Invalid invalid -> {
                deadLetterProducer.reject(event, String.join("; ", invalid.reasons()), event.source());
                yield ResponseEntity
                        .badRequest()
                        .body(Map.of(
                                "eventId", event.eventId(),
                                "status", "rejected",
                                "reasons", invalid.reasons()
                        ));
            }
        };
    }
}
