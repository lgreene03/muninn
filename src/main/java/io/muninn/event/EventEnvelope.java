package io.muninn.event;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID envelopeId,
        String topic,
        int partition,
        long offset,
        Instant ingestedAt,
        Event event
) {

    public static EventEnvelope wrap(String topic, int partition, long offset, Event event) {
        return new EventEnvelope(
                UUID.randomUUID(),
                topic,
                partition,
                offset,
                Instant.now(),
                event
        );
    }
}
