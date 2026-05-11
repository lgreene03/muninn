package io.muninn.shared.exception;

import java.util.UUID;

/**
 * Thrown when event ingestion fails — publish errors, adapter failures, or dead-letter routing issues.
 */
public class IngestionException extends MuninnException {

    private final UUID eventId;

    public IngestionException(UUID eventId, String message) {
        super(message);
        this.eventId = eventId;
    }

    public IngestionException(UUID eventId, String message, Throwable cause) {
        super(message, cause);
        this.eventId = eventId;
    }

    public UUID eventId() {
        return eventId;
    }
}
