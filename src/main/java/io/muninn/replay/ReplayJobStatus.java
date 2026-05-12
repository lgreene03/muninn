package io.muninn.replay;

/**
 * Lifecycle states of a {@link ReplayJob}.
 *
 * <p>Transitions are strictly forward: {@code PENDING} → {@code RUNNING} →
 * ({@code COMPLETED} | {@code FAILED}). A job never moves backwards.</p>
 */
public enum ReplayJobStatus {
    /** Submitted but not yet started. */
    PENDING,
    /** Currently executing on a worker. */
    RUNNING,
    /** Finished successfully; outputs are in the replay topic. */
    COMPLETED,
    /** Terminated by an error before consuming the full range. */
    FAILED
}
