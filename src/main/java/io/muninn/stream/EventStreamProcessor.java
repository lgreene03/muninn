package io.muninn.stream;

import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@link MarketEvent}s from Redpanda and routes them to the event sink.
 *
 * <p>This is a minimal consumer for Phase 1. It will be extended in Phase 3
 * with watermark tracking and feature-engine integration.</p>
 */
@Component
public class EventStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessor.class);

    private final EventSink eventSink;

    public EventStreamProcessor(EventSink eventSink) {
        this.eventSink = eventSink;
    }

    @KafkaListener(
            topics = "${muninn.stream.topics:events.trade}",
            groupId = "${muninn.stream.group-id:muninn-processor}"
    )
    public void process(ConsumerRecord<String, MarketEvent> record) {
        MarketEvent event = record.value();

        log.atDebug()
                .addKeyValue("eventId", event.eventId())
                .addKeyValue("topic", record.topic())
                .addKeyValue("partition", record.partition())
                .addKeyValue("offset", record.offset())
                .log("Processing market event");

        eventSink.accept(event);
    }
}
