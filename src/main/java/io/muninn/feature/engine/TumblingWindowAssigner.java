package io.muninn.feature.engine;

import java.time.Duration;
import java.time.Instant;

/**
 * Assigns events to epoch-aligned tumbling windows.
 *
 * <p>A tumbling window is a non-overlapping, fixed-size window aligned to the Unix epoch.
 * For a 1-minute window, boundaries are at 00:00, 01:00, 02:00, etc. regardless
 * of when the feature engine started.</p>
 *
 * <p>This class is a pure function — no state, no IO, no clock reads. Safe for use
 * in both live and replay paths.</p>
 */
public final class TumblingWindowAssigner {

    private TumblingWindowAssigner() {
        // Utility class — no instantiation
    }

    /**
     * Compute the window start for a given event time.
     *
     * <p>The window is aligned to the Unix epoch. For a 1-minute window,
     * an event at 14:00:37.500Z belongs to the window starting at 14:00:00.000Z.</p>
     *
     * @param eventTime      the event time
     * @param windowDuration the window size (e.g., {@code Duration.ofMinutes(1)})
     * @return the inclusive start of the window containing this event
     * @throws IllegalArgumentException if windowDuration is zero or negative
     */
    public static Instant windowStart(Instant eventTime, Duration windowDuration) {
        validateInputs(eventTime, windowDuration);

        long windowMillis = windowDuration.toMillis();
        long eventMillis = eventTime.toEpochMilli();
        long windowStartMillis = (eventMillis / windowMillis) * windowMillis;
        return Instant.ofEpochMilli(windowStartMillis);
    }

    /**
     * Compute the window end for a given event time.
     *
     * <p>The window end is exclusive: an event at exactly 14:01:00.000Z belongs
     * to the next window (14:01:00–14:02:00), not the previous one.</p>
     *
     * @param eventTime      the event time
     * @param windowDuration the window size
     * @return the exclusive end of the window containing this event
     */
    public static Instant windowEnd(Instant eventTime, Duration windowDuration) {
        return windowStart(eventTime, windowDuration).plus(windowDuration);
    }

    /**
     * Compute both window boundaries as a {@link WindowBounds} record.
     *
     * @param eventTime      the event time
     * @param windowDuration the window size
     * @return the window boundaries
     */
    public static WindowBounds assign(Instant eventTime, Duration windowDuration) {
        Instant start = windowStart(eventTime, windowDuration);
        return new WindowBounds(start, start.plus(windowDuration));
    }

    /**
     * Immutable window boundary pair.
     *
     * @param start inclusive start
     * @param end   exclusive end
     */
    public record WindowBounds(Instant start, Instant end) {
        public WindowBounds {
            if (start == null || end == null) throw new IllegalArgumentException("Window bounds must not be null");
            if (!start.isBefore(end)) throw new IllegalArgumentException("start must be before end");
        }
    }

    private static void validateInputs(Instant eventTime, Duration windowDuration) {
        if (eventTime == null) throw new IllegalArgumentException("eventTime must not be null");
        if (windowDuration == null) throw new IllegalArgumentException("windowDuration must not be null");
        if (windowDuration.isZero() || windowDuration.isNegative()) {
            throw new IllegalArgumentException("windowDuration must be positive");
        }
    }
}
