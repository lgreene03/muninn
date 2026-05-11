package io.muninn.stream;

import io.muninn.event.Event;
import io.muninn.event.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EventStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(EventStreamProcessor.class);

    private final EventSink eventSink;

    public EventStreamProcessor(EventSink eventSink) {
        this.eventSink = eventSink;
    }

    @KafkaListener(topics = "${muninn.stream.topics:events}", groupId = "${muninn.stream.group-id:muninn-processor}")
    public void process(ConsumerRecord<String, Event> record) {
        Event event = record.value();
        EventEnvelope envelope = EventEnvelope.wrap(
                record.topic(), record.partition(), record.offset(), event
        );

        log.debug("Processing event {} from {}[{}]@{}",
                event.eventId(), record.topic(), record.partition(), record.offset());

        eventSink.accept(envelope);
    }
}
