package io.muninn.ingestion;

import java.util.UUID;

public class EventPublishException extends RuntimeException {

    private final UUID eventId;

    public EventPublishException(UUID eventId, Throwable cause) {
        super("Failed to publish event: " + eventId, cause);
        this.eventId = eventId;
    }

    public UUID getEventId() {
        return eventId;
    }
}
