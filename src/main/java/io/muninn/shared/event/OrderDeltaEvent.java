package io.muninn.shared.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.muninn.shared.instrument.Instrument;

import java.time.Instant;
import java.util.UUID;

/**
 * An individual order modification event for Level 3 (Market by Order) data.
 *
 * <p>Represents a single order being added, modified, or deleted from the order book.
 * This is the atomic building block for deterministic L3 order book reconstruction.</p>
 */
public record OrderDeltaEvent(
        @JsonProperty(required = true) UUID eventId,
        @JsonProperty(required = true) Instant eventTime,
        @JsonProperty(required = true) Instant ingestTime,
        @JsonProperty(required = true) String source,
        @JsonProperty(required = true) Instrument instrument,
        long sequenceNumber,
        int schemaVersion,
        @JsonProperty(required = true) String orderId,
        @JsonProperty(required = true) Side side,
        @JsonProperty(required = true) double price,
        @JsonProperty(required = true) double quantity,
        @JsonProperty(required = true) Action action
) implements MarketEvent {

    public enum Action {
        ADD,
        MODIFY,
        DELETE
    }

    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String TOPIC = "events.book.delta";

    public OrderDeltaEvent {
        if (eventId == null) throw new IllegalArgumentException("eventId is required");
        if (eventTime == null) throw new IllegalArgumentException("eventTime is required");
        if (ingestTime == null) throw new IllegalArgumentException("ingestTime is required");
        if (source == null || source.isBlank()) throw new IllegalArgumentException("source is required");
        if (instrument == null) throw new IllegalArgumentException("instrument is required");
        if (orderId == null || orderId.isBlank()) throw new IllegalArgumentException("orderId is required");
        if (side == null) throw new IllegalArgumentException("side is required");
        if (price <= 0 && action != Action.DELETE) throw new IllegalArgumentException("price must be positive for non-delete actions");
        if (quantity < 0) throw new IllegalArgumentException("quantity cannot be negative");
        if (action == null) throw new IllegalArgumentException("action is required");
    }

    @Override
    public String topicName() {
        return TOPIC;
    }
}
