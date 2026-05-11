package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;

import java.time.Instant;
import java.util.Optional;

/**
 * The boundary between the feature engine and its data source.
 *
 * <p>Both live and replay paths implement this interface. The feature engine
 * <strong>cannot</strong> tell which it is reading from — if it needs to know,
 * that is a design failure (per DETERMINISTIC_REPLAY.md).</p>
 *
 * <p>Implementations must emit events in event-time order within each partition
 * and maintain per-partition watermarks.</p>
 */
public interface EventSource {

    /**
     * Poll for the next event from the source.
     *
     * @return the next event, or empty if no event is available within the poll timeout
     */
    Optional<PartitionedEvent> poll();

    /**
     * @return true if the source may produce more events
     */
    boolean hasMore();

    /**
     * Start consuming events.
     */
    void start();

    /**
     * Stop consuming events and release resources.
     */
    void stop();

    /**
     * An event tagged with its source partition and offset, for watermark and checkpoint tracking.
     *
     * @param event     the market event
     * @param partition the source partition number
     * @param offset    the Kafka offset of this event
     */
    record PartitionedEvent(MarketEvent event, int partition, long offset) {
        public PartitionedEvent {
            if (event == null) throw new IllegalArgumentException("event must not be null");
        }
    }
}
