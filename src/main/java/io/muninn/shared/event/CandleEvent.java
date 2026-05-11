package io.muninn.shared.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.muninn.shared.instrument.Instrument;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * A time-bucketed OHLCV candle summary.
 *
 * <p>May be produced by the exchange (preferred) or computed by the feature engine
 * from {@link TradeEvent}s. When computed internally, it is reproducible from the
 * underlying trades via deterministic replay.</p>
 *
 * @param eventId        UUIDv7 event identifier
 * @param eventTime      the candle's close time (exchange-reported)
 * @param ingestTime     when Muninn observed this event
 * @param source         adapter identifier
 * @param instrument     the instrument this candle covers
 * @param sequenceNumber monotonic per {@code (source, instrument)}
 * @param schemaVersion  schema version (currently 1)
 * @param open           opening price
 * @param high           highest price in the window
 * @param low            lowest price in the window
 * @param close          closing price
 * @param volume         total volume in the window (base asset)
 * @param windowStart    start of the candle window (inclusive)
 * @param windowEnd      end of the candle window (exclusive)
 * @param windowDuration duration of the candle window
 */
public record CandleEvent(
        @JsonProperty(required = true) UUID eventId,
        @JsonProperty(required = true) Instant eventTime,
        @JsonProperty(required = true) Instant ingestTime,
        @JsonProperty(required = true) String source,
        @JsonProperty(required = true) Instrument instrument,
        long sequenceNumber,
        int schemaVersion,
        @JsonProperty(required = true) BigDecimal open,
        @JsonProperty(required = true) BigDecimal high,
        @JsonProperty(required = true) BigDecimal low,
        @JsonProperty(required = true) BigDecimal close,
        @JsonProperty(required = true) BigDecimal volume,
        @JsonProperty(required = true) Instant windowStart,
        @JsonProperty(required = true) Instant windowEnd,
        @JsonProperty(required = true) Duration windowDuration
) implements MarketEvent {

    public static final int CURRENT_SCHEMA_VERSION = 1;
    public static final String TOPIC = "events.candle";

    public CandleEvent {
        if (eventId == null) throw new IllegalArgumentException("CandleEvent eventId is required");
        if (eventTime == null) throw new IllegalArgumentException("CandleEvent eventTime is required");
        if (ingestTime == null) throw new IllegalArgumentException("CandleEvent ingestTime is required");
        if (source == null || source.isBlank()) throw new IllegalArgumentException("CandleEvent source is required");
        if (instrument == null) throw new IllegalArgumentException("CandleEvent instrument is required");
        if (open == null) throw new IllegalArgumentException("CandleEvent open is required");
        if (high == null) throw new IllegalArgumentException("CandleEvent high is required");
        if (low == null) throw new IllegalArgumentException("CandleEvent low is required");
        if (close == null) throw new IllegalArgumentException("CandleEvent close is required");
        if (volume == null) throw new IllegalArgumentException("CandleEvent volume is required");
        if (windowStart == null) throw new IllegalArgumentException("CandleEvent windowStart is required");
        if (windowEnd == null) throw new IllegalArgumentException("CandleEvent windowEnd is required");
        if (windowDuration == null) throw new IllegalArgumentException("CandleEvent windowDuration is required");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("CandleEvent windowStart must be before windowEnd");
        if (high.compareTo(low) < 0) throw new IllegalArgumentException("CandleEvent high must be >= low");
    }

    @Override
    public String topicName() {
        return TOPIC;
    }
}
