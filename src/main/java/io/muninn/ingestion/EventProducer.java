package io.muninn.ingestion;

import io.muninn.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class EventProducer {

    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

    private final KafkaTemplate<String, Event> kafkaTemplate;

    public EventProducer(KafkaTemplate<String, Event> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<Void> publish(String topic, Event event) {
        String key = event.partitionKey() != null ? event.partitionKey() : event.eventId().toString();
        return kafkaTemplate.send(topic, key, event)
                .thenAccept(result -> {
                    var metadata = result.getRecordMetadata();
                    log.debug("Published event {} to {}[{}]@{}",
                            event.eventId(), metadata.topic(), metadata.partition(), metadata.offset());
                })
                .exceptionally(ex -> {
                    log.error("Failed to publish event {}: {}", event.eventId(), ex.getMessage());
                    throw new EventPublishException(event.eventId(), ex);
                });
    }
}
