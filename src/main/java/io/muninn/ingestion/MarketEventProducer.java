package io.muninn.ingestion;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.exception.IngestionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Publishes validated {@link MarketEvent}s to their canonical Redpanda topics.
 *
 * <p>Uses idempotent producers ({@code enable.idempotence=true}, {@code acks=all})
 * and routes events to their type-specific topic via {@link MarketEvent#topicName()}.</p>
 */
@Service
public class MarketEventProducer {

    private static final Logger log = LoggerFactory.getLogger(MarketEventProducer.class);

    private final KafkaTemplate<String, MarketEvent> kafkaTemplate;

    public MarketEventProducer(KafkaTemplate<String, MarketEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish a validated market event to its canonical topic.
     *
     * @param event the validated event to publish
     * @return a future that completes when the broker acknowledges the write
     * @throws IngestionException if the publish fails
     */
    public CompletableFuture<Void> publish(MarketEvent event) {
        String topic = event.topicName();
        String key = event.instrument().symbol();

        return kafkaTemplate.send(topic, key, event)
                .thenAccept(result -> {
                    var metadata = result.getRecordMetadata();
                    log.atDebug()
                            .addKeyValue("eventId", event.eventId())
                            .addKeyValue("topic", metadata.topic())
                            .addKeyValue("partition", metadata.partition())
                            .addKeyValue("offset", metadata.offset())
                            .log("Published market event");
                })
                .exceptionally(ex -> {
                    log.atError()
                            .addKeyValue("eventId", event.eventId())
                            .addKeyValue("topic", topic)
                            .addKeyValue("error", ex.getMessage())
                            .log("Failed to publish market event");
                    throw new IngestionException(event.eventId(),
                            "Failed to publish event to " + topic, ex);
                });
    }
}
