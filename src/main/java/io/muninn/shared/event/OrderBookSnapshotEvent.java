package io.muninn.shared.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.muninn.shared.instrument.Instrument;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A point-in-time order-book state, capped to a configured depth.
 *
 * <p>Bid and ask lists are immutable copies. Bids are sorted price-descending (best bid first),
 * asks are sorted price-ascending (best ask first).</p>
 *
 * @param eventId        UUIDv7 event identifier
 * @param eventTime      exchange-reported snapshot timestamp
 * @param ingestTime     when Muninn observed this event
 * @param source         adapter identifier
 * @param instrument     the instrument this book covers
 * @param sequenceNumber monotonic per {@code (source, instrument)}
 * @param schemaVersion  schema version (currently 1)
 * @param bids           bid levels, price-descending
 * @param asks           ask levels, price-ascending
 * @param depth          the configured depth of this snapshot
 */
public record OrderBookSnapshotEvent(
        @JsonProperty(required = true) UUID eventId,
        @JsonProperty(required = true) Instant eventTime,
        @JsonProperty(required = true) Instant ingestTime,
        @JsonProperty(required = true) String source,
        @JsonProperty(required = true) Instrument instrument,
        long sequenceNumber,
        int schemaVersion,
        @JsonProperty(required = true) List<PriceLevel> bids,
        @JsonProperty(required = true) List<PriceLevel> asks,
        int depth
) implements MarketEvent {

    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String TOPIC = "events.book.snapshot";

    public OrderBookSnapshotEvent {
        if (eventId == null) throw new IllegalArgumentException("OrderBookSnapshotEvent eventId is required");
        if (eventTime == null) throw new IllegalArgumentException("OrderBookSnapshotEvent eventTime is required");
        if (ingestTime == null) throw new IllegalArgumentException("OrderBookSnapshotEvent ingestTime is required");
        if (source == null || source.isBlank()) throw new IllegalArgumentException("OrderBookSnapshotEvent source is required");
        if (instrument == null) throw new IllegalArgumentException("OrderBookSnapshotEvent instrument is required");
        if (bids == null) throw new IllegalArgumentException("OrderBookSnapshotEvent bids is required");
        if (asks == null) throw new IllegalArgumentException("OrderBookSnapshotEvent asks is required");
        if (depth <= 0) throw new IllegalArgumentException("OrderBookSnapshotEvent depth must be positive");
        // Defensive copies — immutability guarantee
        bids = List.copyOf(bids);
        asks = List.copyOf(asks);
    }

    @Override
    public String topicName() {
        return TOPIC;
    }
}
