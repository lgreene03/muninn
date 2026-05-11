package io.muninn.shared.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.muninn.shared.instrument.Instrument;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * A single executed trade reported by an exchange.
 *
 * <p>Immutable, validated at construction. Prices and sizes use {@link BigDecimal}
 * to avoid floating-point precision loss — a requirement for deterministic replay.</p>
 *
 * @param eventId        UUIDv7 event identifier
 * @param eventTime      exchange-reported trade timestamp
 * @param ingestTime     when Muninn observed this event
 * @param source         adapter identifier (e.g., {@code "binance.spot.v1"})
 * @param instrument     the instrument traded
 * @param sequenceNumber monotonic per {@code (source, instrument)}
 * @param schemaVersion  schema version (currently 1)
 * @param price          trade execution price
 * @param size           trade size in base asset units
 * @param side           aggressor side (BUY, SELL, or UNKNOWN)
 * @param exchangeTradeId the exchange's native trade identifier
 */
public record TradeEvent(
        @JsonProperty(required = true) UUID eventId,
        @JsonProperty(required = true) Instant eventTime,
        @JsonProperty(required = true) Instant ingestTime,
        @JsonProperty(required = true) String source,
        @JsonProperty(required = true) Instrument instrument,
        long sequenceNumber,
        int schemaVersion,
        @JsonProperty(required = true) BigDecimal price,
        @JsonProperty(required = true) BigDecimal size,
        @JsonProperty(required = true) Side side,
        String exchangeTradeId
) implements MarketEvent, java.io.Serializable {

    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String TOPIC = "events.trade";

    public TradeEvent {
        if (eventId == null) throw new IllegalArgumentException("TradeEvent eventId is required");
        if (eventTime == null) throw new IllegalArgumentException("TradeEvent eventTime is required");
        if (ingestTime == null) throw new IllegalArgumentException("TradeEvent ingestTime is required");
        if (source == null || source.isBlank()) throw new IllegalArgumentException("TradeEvent source is required");
        if (instrument == null) throw new IllegalArgumentException("TradeEvent instrument is required");
        if (price == null) throw new IllegalArgumentException("TradeEvent price is required");
        if (size == null) throw new IllegalArgumentException("TradeEvent size is required");
        if (side == null) throw new IllegalArgumentException("TradeEvent side is required");
        if (price.signum() <= 0) throw new IllegalArgumentException("TradeEvent price must be positive");
        if (size.signum() < 0) throw new IllegalArgumentException("TradeEvent size must not be negative");
    }

    @Override
    public String topicName() {
        return TOPIC;
    }
}
