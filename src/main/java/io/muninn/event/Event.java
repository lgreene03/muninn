package io.muninn.event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record Event(
        UUID eventId,
        String eventType,
        Instant eventTime,
        long sequenceNumber,
        String source,
        String partitionKey,
        Map<String, Object> payload
) {

    public Event {
        if (eventId == null) throw new IllegalArgumentException("eventId is required");
        if (eventType == null || eventType.isBlank()) throw new IllegalArgumentException("eventType is required");
        if (eventTime == null) throw new IllegalArgumentException("eventTime is required");
        if (source == null || source.isBlank()) throw new IllegalArgumentException("source is required");
        if (payload == null) throw new IllegalArgumentException("payload is required");
    }

    public static Event create(String eventType, String source, String partitionKey, Map<String, Object> payload) {
        return new Event(
                UUID.randomUUID(),
                eventType,
                Instant.now(),
                0L,
                source,
                partitionKey,
                Map.copyOf(payload)
        );
    }
}
