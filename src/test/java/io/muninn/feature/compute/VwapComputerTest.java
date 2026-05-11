package io.muninn.feature.compute;

import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link VwapComputer}.
 * Pure-function tests — no Spring context, no IO.
 */
class VwapComputerTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instant WINDOW_START = Instant.parse("2026-05-11T14:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-05-11T14:01:00Z");
    private static final String CODE_VERSION = "test-sha-001";

    @Test
    void compute_singleTrade_returnsPrice() {
        TradeEvent trade = trade("67500.00", "1.0", "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(trade));

        FeatureComputedEvent result = VwapComputer.compute(batch, CODE_VERSION);

        // VWAP with single trade = trade price
        assertThat(result.value()).isEqualByComparingTo("67500.00000000");
        assertThat(result.featureName()).isEqualTo("vwap.1m");
        assertThat(result.featureVersion()).isEqualTo("v1");
        assertThat(result.windowStart()).isEqualTo(WINDOW_START);
        assertThat(result.windowEnd()).isEqualTo(WINDOW_END);
        assertThat(result.inputEventIds()).hasSize(1);
        assertThat(result.codeVersion()).isEqualTo(CODE_VERSION);
    }

    @Test
    void compute_multipleTrades_returnsVolumeWeightedAverage() {
        // VWAP = (100*1 + 200*3) / (1+3) = 700/4 = 175
        TradeEvent t1 = trade("100.00", "1.0", "2026-05-11T14:00:10Z");
        TradeEvent t2 = trade("200.00", "3.0", "2026-05-11T14:00:20Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(t1, t2));

        FeatureComputedEvent result = VwapComputer.compute(batch, CODE_VERSION);

        assertThat(result.value()).isEqualByComparingTo("175.00000000");
        assertThat(result.inputEventIds()).hasSize(2);
    }

    @Test
    void compute_threeTrades_correctVwap() {
        // VWAP = (67500*0.5 + 67510*1.5 + 67490*0.25) / (0.5+1.5+0.25)
        //      = (33750 + 101265 + 16872.5) / 2.25
        //      = 151887.5 / 2.25
        //      = 67505.555...
        TradeEvent t1 = trade("67500.00", "0.50", "2026-05-11T14:00:05Z");
        TradeEvent t2 = trade("67510.00", "1.50", "2026-05-11T14:00:15Z");
        TradeEvent t3 = trade("67490.00", "0.25", "2026-05-11T14:00:25Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(t1, t2, t3));

        FeatureComputedEvent result = VwapComputer.compute(batch, CODE_VERSION);

        assertThat(result.value()).isEqualByComparingTo("67505.55555556"); // rounded HALF_EVEN
        assertThat(result.inputEventIds()).hasSize(3);
    }

    @Test
    void compute_zeroSizeTrades_fallsBackToSimpleAverage() {
        // All trades have size=0 — should use simple average of prices
        TradeEvent t1 = trade("67500.00", "0", "2026-05-11T14:00:10Z");
        TradeEvent t2 = trade("67600.00", "0", "2026-05-11T14:00:20Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(t1, t2));

        FeatureComputedEvent result = VwapComputer.compute(batch, CODE_VERSION);

        // Simple average: (67500 + 67600) / 2 = 67550
        assertThat(result.value()).isEqualByComparingTo("67550.00000000");
    }

    @Test
    void compute_emptyBatch_throwsIllegalArgument() {
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of());

        assertThatThrownBy(() -> VwapComputer.compute(batch, CODE_VERSION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty window");
    }

    @Test
    void compute_nullCodeVersion_throwsIllegalArgument() {
        TradeEvent trade = trade("67500.00", "1.0", "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(trade));

        assertThatThrownBy(() -> VwapComputer.compute(batch, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("codeVersion");
    }

    @Test
    void compute_isDeterministic_sameInputsSameOutput() {
        TradeEvent t1 = trade("67500.00", "0.50", "2026-05-11T14:00:05Z");
        TradeEvent t2 = trade("67510.00", "1.50", "2026-05-11T14:00:15Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(t1, t2));

        FeatureComputedEvent result1 = VwapComputer.compute(batch, CODE_VERSION);
        FeatureComputedEvent result2 = VwapComputer.compute(batch, CODE_VERSION);

        // Same value, same provenance (different eventIds due to UUIDv7 generation)
        assertThat(result1.value()).isEqualByComparingTo(result2.value());
        assertThat(result1.inputEventIds()).isEqualTo(result2.inputEventIds());
        assertThat(result1.windowStart()).isEqualTo(result2.windowStart());
        assertThat(result1.windowEnd()).isEqualTo(result2.windowEnd());
    }

    @Test
    void compute_bigDecimalPrecisionPreserved() {
        // Very precise prices — should not lose precision
        TradeEvent t1 = trade("67523.12345678", "0.00000001", "2026-05-11T14:00:10Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(t1));

        FeatureComputedEvent result = VwapComputer.compute(batch, CODE_VERSION);

        assertThat(result.value()).isEqualByComparingTo("67523.12345678");
    }

    // --- Helper ---

    private TradeEvent trade(String price, String size, String eventTime) {
        return new TradeEvent(
                UUIDv7.generate(),
                Instant.parse(eventTime),
                Instant.parse(eventTime),
                "test",
                BTC_USDT,
                1L, 1,
                new BigDecimal(price),
                new BigDecimal(size),
                Side.BUY,
                "t-" + System.nanoTime()
        );
    }
}
