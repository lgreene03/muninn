package io.muninn.shared.exception;

import java.util.List;

/**
 * Thrown when an event fails validation at the ingestion boundary.
 * Carries the list of validation failure reasons for structured logging and dead-letter routing.
 */
public class ValidationException extends MuninnException {

    private final List<String> reasons;

    public ValidationException(List<String> reasons) {
        super("Event validation failed: " + String.join("; ", reasons));
        this.reasons = List.copyOf(reasons);
    }

    public List<String> reasons() {
        return reasons;
    }
}
