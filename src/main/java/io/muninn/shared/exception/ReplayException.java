package io.muninn.shared.exception;

/**
 * Thrown when a replay operation fails — checkpoint mismatches, divergence detection errors, job failures.
 */
public class ReplayException extends MuninnException {

    public ReplayException(String message) {
        super(message);
    }

    public ReplayException(String message, Throwable cause) {
        super(message, cause);
    }
}
