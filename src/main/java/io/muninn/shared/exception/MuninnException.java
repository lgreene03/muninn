package io.muninn.shared.exception;

/**
 * Root exception for all Muninn domain errors.
 * Subclasses represent specific failure domains (validation, ingestion, storage, replay).
 * Always throw a typed subclass, never this class directly.
 */
public abstract class MuninnException extends RuntimeException {

    protected MuninnException(String message) {
        super(message);
    }

    protected MuninnException(String message, Throwable cause) {
        super(message, cause);
    }
}
