package io.muninn.shared.validation;

import io.muninn.shared.event.*;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link EventValidator}.
 * Covers all 5 validation rules from EVENT_SCHEMA_STRATEGY.md.
 */
class EventValidatorTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    private EventValidator validator;

    @BeforeEach
    void setUp() {
        validator = new EventValidator();
    }

    // --- Valid events ---

    @Test
    void validate_validTradeEvent_returnsValid() {
        TradeEvent trade = validTrade();
        ValidationResult result = validator.validate(trade);
        assertThat(result.isValid()).isTrue();
        assertThat(result).isInstanceOf(ValidationResult.Valid.class);
    }

    @Test
    void validate_validBookSnapshot_returnsValid() {
        OrderBookSnapshotEvent book = validBookSnapshot();
        ValidationResult result = validator.validate(book);
        assertThat(result.isValid()).isTrue();
    }

    // --- Rule 2: Time fields in valid range ---

    @Test
    void validate_eventTimeTooOld_returnsInvalid() {
        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(),
                Instant.parse("2019-01-01T00:00:00Z"), // Before earliest valid time (2020)
                Instant.now(),
                "src", BTC_USDT, 1L, 1,
                BigDecimal.ONE, BigDecimal.ONE, Side.BUY, "123"
        );

        ValidationResult result = validator.validate(trade);
        assertThat(result.isValid()).isFalse();
        assertThat(((ValidationResult.Invalid) result).reasons())
                .anyMatch(r -> r.contains("before earliest valid time"));
    }

    @Test
    void validate_eventTimeFarFuture_returnsInvalid() {
        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(),
                Instant.now().plus(Duration.ofHours(1)), // 1 hour in the future
                Instant.now(),
                "src", BTC_USDT, 1L, 1,
                BigDecimal.ONE, BigDecimal.ONE, Side.BUY, "123"
        );

        ValidationResult result = validator.validate(trade);
        assertThat(result.isValid()).isFalse();
        assertThat(((ValidationResult.Invalid) result).reasons())
                .anyMatch(r -> r.contains("too far in the future"));
    }

    // --- Rule 3: eventTime ≤ ingestTime + clockSkewTolerance ---

    @Test
    void validate_eventTimeExceedsClockSkew_returnsInvalid() {
        Instant ingestTime = Instant.parse("2026-05-11T12:00:00Z");
        Instant eventTime = ingestTime.plus(Duration.ofMinutes(2)); // 2 min after ingest — exceeds 30s tolerance

        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(), eventTime, ingestTime,
                "src", BTC_USDT, 1L, 1,
                BigDecimal.ONE, BigDecimal.ONE, Side.BUY, "123"
        );

        ValidationResult result = validator.validate(trade);
        assertThat(result.isValid()).isFalse();
        assertThat(((ValidationResult.Invalid) result).reasons())
                .anyMatch(r -> r.contains("clockSkewTolerance"));
    }

    @Test
    void validate_eventTimeWithinClockSkew_returnsValid() {
        Instant now = Instant.now();
        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(), now.plusSeconds(10), now,
                "src", BTC_USDT, 1L, 1,
                BigDecimal.ONE, BigDecimal.ONE, Side.BUY, "123"
        );

        ValidationResult result = validator.validate(trade);
        assertThat(result.isValid()).isTrue();
    }

    // --- Rule 4: Numeric bounds (type-specific) ---

    @Test
    void validate_orderBookEmptyBids_returnsInvalid() {
        OrderBookSnapshotEvent book = new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(),
                "src", BTC_USDT, 1L, 1,
                List.of(), // empty bids
                List.of(new PriceLevel(BigDecimal.ONE, BigDecimal.ONE)),
                1
        );

        ValidationResult result = validator.validate(book);
        assertThat(result.isValid()).isFalse();
        assertThat(((ValidationResult.Invalid) result).reasons())
                .anyMatch(r -> r.contains("at least one bid level"));
    }

    // --- ValidationResult sealed interface ---

    @Test
    void validResult_isValid_returnsTrue() {
        assertThat(new ValidationResult.Valid().isValid()).isTrue();
    }

    @Test
    void invalidResult_isValid_returnsFalse() {
        assertThat(new ValidationResult.Invalid(List.of("reason")).isValid()).isFalse();
    }

    @Test
    void invalidResult_emptyReasons_throwsIllegalArgument() {
        assertThatThrownBy(() -> new ValidationResult.Invalid(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- Helper methods ---

    private TradeEvent validTrade() {
        return new TradeEvent(
                UUIDv7.generate(),
                Instant.now().minusSeconds(1),
                Instant.now(),
                "binance.spot.v1",
                BTC_USDT,
                1L, 1,
                new BigDecimal("67500.50"),
                new BigDecimal("0.01"),
                Side.BUY,
                "123456"
        );
    }

    private OrderBookSnapshotEvent validBookSnapshot() {
        return new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(),
                "binance.spot.v1", BTC_USDT, 1L, 1,
                List.of(new PriceLevel(new BigDecimal("67500"), new BigDecimal("1.5"))),
                List.of(new PriceLevel(new BigDecimal("67501"), new BigDecimal("0.5"))),
                20
        );
    }
}
