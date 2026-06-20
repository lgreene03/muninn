package io.muninn.feature.engine;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-partition watermarks and computes the global watermark.
 *
 * <p>The global watermark is the minimum of all per-partition watermarks. A window
 * is considered complete when its end time is before the global watermark.</p>
 *
 * <p>Watermarks are monotonic — they never go backward within a partition.
 * An attempt to advance a watermark backward is silently ignored (the event is late).</p>
 *
 * <p>Thread-safe: uses {@link ConcurrentHashMap} for partition state.</p>
 */
public final class WatermarkTracker {

    private final Map<Integer, Instant> partitionWatermarks = new ConcurrentHashMap<>();

    /**
     * Advance the watermark for a specific partition.
     *
     * <p>Only advances if the new watermark is strictly after the current one.
     * Returns {@code true} if the watermark was actually advanced.</p>
     *
     * @param partition the partition number
     * @param eventTime the event time to use as the new watermark candidate
     * @return true if the watermark advanced, false if the event was late
     */
    public boolean advance(int partition, Instant eventTime) {
        if (eventTime == null) throw new IllegalArgumentException("eventTime must not be null");

        return partitionWatermarks.compute(partition, (key, current) -> {
            if (current == null || eventTime.isAfter(current)) {
                return eventTime;
            }
            return current; // monotonic — never go backward
        }).equals(eventTime);
    }

    /**
     * Seed the watermark for a partition to an exact value during checkpoint restore.
     *
     * <p>Unlike {@link #advance}, this sets the watermark unconditionally — it is only
     * intended to re-establish the watermark captured in a checkpoint on engine boot,
     * before any live events are consumed. Do not use on the hot path; {@link #advance}
     * preserves monotonicity there.</p>
     *
     * @param partition the partition number
     * @param watermark the watermark value to seed
     */
    public void seed(int partition, Instant watermark) {
        if (watermark == null) throw new IllegalArgumentException("watermark must not be null");
        partitionWatermarks.put(partition, watermark);
    }

    /**
     * Get the current watermark for a specific partition.
     *
     * @param partition the partition number
     * @return the current watermark, or {@link Instant#MIN} if no events have been seen
     */
    public Instant partitionWatermark(int partition) {
        return partitionWatermarks.getOrDefault(partition, Instant.MIN);
    }

    /**
     * Get the global watermark — the minimum of all per-partition watermarks.
     *
     * <p>If no partitions have been registered, returns {@link Instant#MIN}.</p>
     *
     * @return the global watermark
     */
    public Instant globalWatermark() {
        if (partitionWatermarks.isEmpty()) {
            return Instant.MIN;
        }

        return partitionWatermarks.values().stream()
                .min(Instant::compareTo)
                .orElse(Instant.MIN);
    }

    /**
     * Check whether a window is complete (its end is before the global watermark).
     *
     * @param windowEnd the exclusive end of the window
     * @return true if the window is complete
     */
    public boolean isWindowComplete(Instant windowEnd) {
        return windowEnd.isBefore(globalWatermark()) || windowEnd.equals(globalWatermark());
    }

    /**
     * @return the number of tracked partitions
     */
    public int partitionCount() {
        return partitionWatermarks.size();
    }

    /**
     * Reset all watermarks. Used during testing and checkpoint restore.
     */
    public void reset() {
        partitionWatermarks.clear();
    }
}
