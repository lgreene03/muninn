package io.muninn.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Publishes rejected events to the dead-letter topic with structured failure reasons.
 *
 * <p>Events that fail validation or parsing are routed here rather than silently dropped.
 * The dead-letter topic ({@code events.deadletter}) preserves the raw event data and
 * the rejection reason for debugging and operational visibility.</p>
 */
@Service
public class DeadLetterProducer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterProducer.class);
    private static final String DEAD_LETTER_TOPIC = "events.deadletter";

    private final KafkaTemplate<String, Object> deadLetterTemplate;

    public DeadLetterProducer(KafkaTemplate<String, Object> deadLetterTemplate) {
        this.deadLetterTemplate = deadLetterTemplate;
    }

    /**
     * Send a rejected event to the dead-letter topic.
     *
     * @param rawPayload the original payload (may be a raw JSON string or a partially parsed object)
     * @param reason     structured reason for rejection
     * @param source     the source adapter that produced this event
     */
    public void reject(Object rawPayload, String reason, String source) {
        Map<String, Object> envelope = Map.of(
                "payload", rawPayload != null ? rawPayload : "null",
                "reason", reason,
                "source", source,
                "rejectedAt", java.time.Instant.now().toString()
        );

        deadLetterTemplate.send(DEAD_LETTER_TOPIC, source, envelope)
                .thenAccept(result -> log.atDebug()
                        .addKeyValue("source", source)
                        .addKeyValue("reason", reason)
                        .log("Event sent to dead-letter topic"))
                .exceptionally(ex -> {
                    log.atError()
                            .addKeyValue("source", source)
                            .addKeyValue("reason", reason)
                            .addKeyValue("error", ex.getMessage())
                            .log("Failed to send event to dead-letter topic");
                    return null;
                });
    }
}
