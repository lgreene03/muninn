package io.muninn.feature.checkpoint;

import io.muninn.feature.engine.WatermarkTracker;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Immutable snapshot of feature-engine state at a known watermark.
 *
 * <p>Contains all the information needed to restore the engine to a consistent
 * point: open window states, consumer offsets, and the watermark at checkpoint time.</p>
 *
 * <p>Serialized to MinIO and indexed in PostgreSQL. A checkpoint is only valid for
 * the exact code version that produced it (per DETERMINISTIC_REPLAY.md §Checkpoints).</p>
 *
 * @param featureName    the feature this checkpoint belongs to
 * @param featureVersion the code version that produced this checkpoint
 * @param watermark      the global watermark at checkpoint time
 * @param windowStates   the open (incomplete) windows and their accumulated state
 * @param consumerOffsets the Kafka consumer offsets at checkpoint time
 * @param createdAt      when this checkpoint was written (wall-clock, for SLA tracking only)
 */
public record CheckpointState(
        String featureName,
        String featureVersion,
        Instant watermark,
        List<WindowState> windowStates,
        Map<Integer, Long> consumerOffsets,
        Instant createdAt
) implements Serializable {

    private static final long serialVersionUID = 1L;

    public CheckpointState {
        if (featureName == null || featureName.isBlank()) throw new IllegalArgumentException("featureName is required");
        if (featureVersion == null || featureVersion.isBlank()) throw new IllegalArgumentException("featureVersion is required");
        if (watermark == null) throw new IllegalArgumentException("watermark is required");
        if (windowStates == null) throw new IllegalArgumentException("windowStates is required");
        if (consumerOffsets == null) throw new IllegalArgumentException("consumerOffsets is required");
        if (createdAt == null) throw new IllegalArgumentException("createdAt is required");
        windowStates = List.copyOf(windowStates);
        consumerOffsets = Map.copyOf(consumerOffsets);
    }

    /**
     * State of a single incomplete window at checkpoint time.
     *
     * @param windowStart inclusive start of the window
     * @param windowEnd   exclusive end of the window
     * @param events      the trade events buffered in this window so far
     */
    public record WindowState(
            Instant windowStart,
            Instant windowEnd,
            List<io.muninn.shared.event.TradeEvent> events
    ) implements Serializable {

        private static final long serialVersionUID = 1L;

        public WindowState {
            if (windowStart == null) throw new IllegalArgumentException("windowStart is required");
            if (windowEnd == null) throw new IllegalArgumentException("windowEnd is required");
            if (events == null) throw new IllegalArgumentException("events is required");
            events = List.copyOf(events);
        }
    }
}
