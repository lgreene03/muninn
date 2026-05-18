package io.muninn.shared.validation;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.event.CandleEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Validates {@link MarketEvent}s at the ingestion boundary.
 *
 * <p>Implements the five validation rules from EVENT_SCHEMA_STRATEGY.md §Validation:</p>
 * <ol>
 *   <li>Required fields present (enforced by record constructors, but checked here for deserialized events)</li>
 *   <li>Time fields in valid range (not {@code Instant.MIN}, not far-future)</li>
 *   <li>{@code eventTime ≤ ingestTime + clockSkewTolerance}</li>
 *   <li>Numeric fields within sane bounds</li>
 *   <li>Reference data resolvable (instrument exists, exchange exists) — deferred to Phase 2</li>
 * </ol>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class EventValidator {

    /** Maximum allowed clock skew between event time and ingest time. */
    private final Duration clockSkewTolerance;

    /** Events claiming to be more than this far in the future are rejected. */
    private final Duration maxFutureOffset;

    /** Earliest valid event time — events before this are likely corrupt data. */
    private final Instant earliestValidTime;

    public EventValidator(Duration clockSkewTolerance, Duration maxFutureOffset, Instant earliestValidTime) {
        this.clockSkewTolerance = clockSkewTolerance;
        this.maxFutureOffset = maxFutureOffset;
        this.earliestValidTime = earliestValidTime;
    }

    /**
     * Create a validator with sensible defaults.
     * Clock skew tolerance: 30 seconds. Max future offset: 5 minutes.
     * Earliest valid time: 2020-01-01T00:00:00Z.
     */
    public EventValidator() {
        this(
                Duration.ofSeconds(30),
                Duration.ofMinutes(5),
                Instant.parse("2020-01-01T00:00:00Z")
        );
    }

    /**
     * Validate a market event. Returns {@link ValidationResult.Valid} if all checks pass,
     * or {@link ValidationResult.Invalid} with a list of failure reasons.
     */
    public ValidationResult validate(MarketEvent event) {
        List<String> reasons = new ArrayList<>();

        validateCommonFields(event, reasons);

        switch (event) {
            case TradeEvent trade -> validateTrade(trade, reasons);
            case CandleEvent candle -> validateCandle(candle, reasons);
            case OrderBookSnapshotEvent book -> validateOrderBook(book, reasons);
            case io.muninn.shared.event.OrderDeltaEvent delta -> validateOrderDelta(delta, reasons);
        }

        if (reasons.isEmpty()) {
            return new ValidationResult.Valid();
        }
        return new ValidationResult.Invalid(reasons);
    }

    private void validateCommonFields(MarketEvent event, List<String> reasons) {
        // Rule 1: Required fields (record constructors enforce non-null, but check after deserialization)
        if (event.eventId() == null) {
            reasons.add("eventId is required");
        }
        if (event.source() == null || event.source().isBlank()) {
            reasons.add("source is required");
        }
        if (event.instrument() == null) {
            reasons.add("instrument is required");
        }

        // Rule 2: Time fields in valid range
        if (event.eventTime() == null) {
            reasons.add("eventTime is required");
        } else {
            if (event.eventTime().isBefore(earliestValidTime)) {
                reasons.add("eventTime is before earliest valid time: " + earliestValidTime);
            }
            Instant now = Instant.now();
            if (event.eventTime().isAfter(now.plus(maxFutureOffset))) {
                reasons.add("eventTime is too far in the future: " + event.eventTime());
            }
        }

        if (event.ingestTime() == null) {
            reasons.add("ingestTime is required");
        }

        // Rule 3: eventTime ≤ ingestTime + clockSkewTolerance
        if (event.eventTime() != null && event.ingestTime() != null) {
            Instant maxAllowedEventTime = event.ingestTime().plus(clockSkewTolerance);
            if (event.eventTime().isAfter(maxAllowedEventTime)) {
                reasons.add("eventTime exceeds ingestTime + clockSkewTolerance: eventTime="
                        + event.eventTime() + ", ingestTime=" + event.ingestTime()
                        + ", tolerance=" + clockSkewTolerance);
            }
        }
    }

    private void validateTrade(TradeEvent trade, List<String> reasons) {
        // Rule 4: Numeric bounds (record constructor enforces price > 0, size >= 0, but re-check)
        if (trade.price() != null && trade.price().signum() <= 0) {
            reasons.add("trade price must be positive: " + trade.price());
        }
        if (trade.size() != null && trade.size().signum() < 0) {
            reasons.add("trade size must not be negative: " + trade.size());
        }
    }

    private void validateCandle(CandleEvent candle, List<String> reasons) {
        if (candle.high() != null && candle.low() != null && candle.high().compareTo(candle.low()) < 0) {
            reasons.add("candle high must be >= low: high=" + candle.high() + ", low=" + candle.low());
        }
        if (candle.windowStart() != null && candle.windowEnd() != null
                && !candle.windowStart().isBefore(candle.windowEnd())) {
            reasons.add("candle windowStart must be before windowEnd");
        }
        if (candle.volume() != null && candle.volume().signum() < 0) {
            reasons.add("candle volume must not be negative: " + candle.volume());
        }
    }

    private void validateOrderBook(OrderBookSnapshotEvent book, List<String> reasons) {
        if (book.bids() == null || book.bids().isEmpty()) {
            reasons.add("order book must have at least one bid level");
        }
        if (book.asks() == null || book.asks().isEmpty()) {
            reasons.add("order book must have at least one ask level");
        }
        if (book.depth() <= 0) {
            reasons.add("order book depth must be positive: " + book.depth());
        }
    }

    private void validateOrderDelta(io.muninn.shared.event.OrderDeltaEvent delta, List<String> reasons) {
        if (delta.price() <= 0 && delta.action() != io.muninn.shared.event.OrderDeltaEvent.Action.DELETE) {
            reasons.add("order delta price must be positive for non-delete actions: " + delta.price());
        }
        if (delta.quantity() < 0) {
            reasons.add("order delta quantity must not be negative: " + delta.quantity());
        }
    }
}
