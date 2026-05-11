package io.muninn.shared.event;

import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link TradeEvent}.
 * Covers construction, immutability, validation, and edge cases.
 */
class TradeEventTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    private TradeEvent validTrade() {
        return new TradeEvent(
                UUIDv7.generate(),
                Instant.parse("2026-05-11T12:00:00Z"),
                Instant.parse("2026-05-11T12:00:00.100Z"),
                "binance.spot.v1",
                BTC_USDT,
                1L,
                1,
                new BigDecimal("67500.50"),
                new BigDecimal("0.01"),
                Side.BUY,
                "123456"
        );
    }

    @Test
    void construct_validTrade_succeeds() {
        TradeEvent trade = validTrade();
        assertThat(trade.eventId()).isNotNull();
        assertThat(trade.price()).isEqualByComparingTo("67500.50");
        assertThat(trade.size()).isEqualByComparingTo("0.01");
        assertThat(trade.side()).isEqualTo(Side.BUY);
        assertThat(trade.instrument().symbol()).isEqualTo("BTC-USDT");
        assertThat(trade.topicName()).isEqualTo("events.trade");
        assertThat(trade.schemaVersion()).isEqualTo(1);
    }

    @Test
    void construct_nullPrice_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, null, BigDecimal.ONE, Side.BUY, "123"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("price is required");
    }

    @Test
    void construct_negativePrice_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, new BigDecimal("-1"), BigDecimal.ONE, Side.BUY, "123"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("price must be positive");
    }

    @Test
    void construct_zeroPrice_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, BigDecimal.ZERO, BigDecimal.ONE, Side.BUY, "123"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("price must be positive");
    }

    @Test
    void construct_negativeSize_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, BigDecimal.ONE, new BigDecimal("-1"), Side.BUY, "123"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size must not be negative");
    }

    @Test
    void construct_zeroSize_succeeds() {
        // Zero-size trades are valid (some exchanges report them)
        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, BigDecimal.ONE, BigDecimal.ZERO, Side.BUY, "123"
        );
        assertThat(trade.size()).isEqualByComparingTo("0");
    }

    @Test
    void construct_nullEventId_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TradeEvent(
                null, Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, BigDecimal.ONE, BigDecimal.ONE, Side.BUY, "123"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("eventId is required");
    }

    @Test
    void construct_blankSource_throwsIllegalArgument() {
        assertThatThrownBy(() -> new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "", BTC_USDT,
                1L, 1, BigDecimal.ONE, BigDecimal.ONE, Side.BUY, "123"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source is required");
    }

    @Test
    void bigDecimalPrecision_isPreserved() {
        BigDecimal precisePrice = new BigDecimal("67500.12345678");
        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(), "src", BTC_USDT,
                1L, 1, precisePrice, BigDecimal.ONE, Side.BUY, "123"
        );
        assertThat(trade.price()).isEqualByComparingTo(precisePrice);
        assertThat(trade.price().scale()).isEqualTo(8);
    }

    @Test
    void implementsMarketEvent() {
        TradeEvent trade = validTrade();
        assertThat(trade).isInstanceOf(MarketEvent.class);
    }
}
