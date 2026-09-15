package io.muninn.feature.microstructure;

import io.muninn.feature.compute.VwapComputer;
import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-computer invariants called out explicitly by the P2 roadmap as cheap checks
 * that catch the classic microstructure-feature bugs:
 *
 * <ul>
 *   <li>{@code microPrice} lies strictly between best bid and best ask — catches the
 *       cross-weighting being written backwards.</li>
 *   <li>{@code vpin != |obi|} on a fixture where they genuinely differ — an explicit
 *       anti-regression guard against huginn's degenerate {@code vpin = |obi|}.</li>
 *   <li>{@code microPrice != vwap} on asymmetric top-of-book sizes — an explicit
 *       anti-regression guard against huginn's degenerate {@code microPrice = vwap}.</li>
 * </ul>
 */
class MicrostructureInvariantsTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instant WINDOW_START = Instant.parse("2026-05-11T14:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-05-11T14:01:00Z");
    private static final String CODE_VERSION = "invariant-test";

    @Test
    void microPrice_liesStrictlyBetweenBestBidAndBestAsk_acrossManyAsymmetricBooks() {
        // A spread of asymmetric top-of-book combinations, not just one lucky case.
        String[][] cases = {
                // bestBid, bidVol, bestAsk, askVol
                {"100.00", "9.00", "101.00", "1.00"},
                {"100.00", "1.00", "101.00", "9.00"},
                {"100.00", "0.30", "100.10", "0.05"},
                {"67500.00", "2.75", "67510.00", "0.40"},
        };

        for (String[] c : cases) {
            BigDecimal bestBid = new BigDecimal(c[0]);
            BigDecimal bestAsk = new BigDecimal(c[2]);
            OrderBookSnapshotEvent snapshot = snapshot(
                    new PriceLevel(bestBid, new BigDecimal(c[1])),
                    new PriceLevel(bestAsk, new BigDecimal(c[3])));
            WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

            FeatureComputedEvent result = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();

            assertThat(result.value())
                    .as("microPrice for bid=%s/%s ask=%s/%s", c[0], c[1], c[2], c[3])
                    .isGreaterThan(bestBid)
                    .isLessThan(bestAsk);
        }
    }

    @Test
    void vpin_differsFromAbsoluteObi_onAFixtureWhereTheyGenuinelyDiverge() {
        // Book state (OBI) and trade flow (VPIN) are independent quantities. Construct
        // a snapshot and a trade sequence, over the SAME window, where they diverge:
        // the book is heavily bid-skewed (positive OBI) while the actually-executed
        // flow in that window is toxic on the SELL side (VPIN driven by sell volume).
        OrderBookSnapshotEvent snapshot = snapshot(
                new PriceLevel(new BigDecimal("100.00"), new BigDecimal("9.00")),
                new PriceLevel(new BigDecimal("101.00"), new BigDecimal("1.00")));

        TradeEvent sell1 = trade("5", Side.SELL, "2026-05-11T14:00:10Z");
        TradeEvent sell2 = trade("5", Side.SELL, "2026-05-11T14:00:20Z");

        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot, sell1, sell2));

        FeatureComputedEvent obiEvent = OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION).orElseThrow();
        VPINComputer.Result vpinResult = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        BigDecimal obi = obiEvent.value();
        BigDecimal vpin = vpinResult.event().orElseThrow().value();

        // obi = (9-1)/10 = 0.8 (positive — book is bid-heavy)
        assertThat(obi).isEqualByComparingTo("0.80000000");
        // vpin = |0-10|/10 = 1.0 (all-sell flow — fully toxic)
        assertThat(vpin).isEqualByComparingTo("1.00000000");

        assertThat(vpin)
                .as("vpin must not degenerate to |obi| — they measure different things")
                .isNotEqualTo(obi.abs());
    }

    @Test
    void microPrice_differsFromVwap_onAsymmetricTopOfBookSizes() {
        // VWAP is a volume-weighted average of EXECUTED TRADE prices; micro-price is a
        // volume-weighted blend of the CURRENT BOOK's best bid/ask. Feed a trade
        // sequence and a book snapshot with deliberately different price levels so the
        // two computers cannot coincidentally agree.
        // VWAP will average trade prices around 100.
        WindowedBatch vwapBatch = new WindowedBatch(WINDOW_START, WINDOW_END,
                List.of(tradeAt("100.00", "1", Side.BUY, "2026-05-11T14:00:05Z"),
                        tradeAt("100.20", "1", Side.BUY, "2026-05-11T14:00:15Z")));

        OrderBookSnapshotEvent snapshot = snapshot(
                new PriceLevel(new BigDecimal("102.00"), new BigDecimal("9.00")),
                new PriceLevel(new BigDecimal("103.00"), new BigDecimal("1.00")));
        WindowedBatch microPriceBatch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent vwap = VwapComputer.compute(vwapBatch, CODE_VERSION);
        FeatureComputedEvent microPrice = MicroPriceComputer.compute(microPriceBatch, CODE_VERSION).orElseThrow();

        assertThat(microPrice.value())
                .as("microPrice must not degenerate to vwap — they read different inputs")
                .isNotEqualTo(vwap.value());
    }

    // --- Helpers ---

    private OrderBookSnapshotEvent snapshot(PriceLevel bid, PriceLevel ask) {
        Instant t = Instant.parse("2026-05-11T14:00:30Z");
        return new OrderBookSnapshotEvent(
                UUIDv7.generate(), t, t, "test", BTC_USDT, 1L, 1, List.of(bid), List.of(ask), 1);
    }

    private TradeEvent trade(String size, Side side, String eventTime) {
        return tradeAt("100.00", size, side, eventTime);
    }

    private TradeEvent tradeAt(String price, String size, Side side, String eventTime) {
        Instant t = Instant.parse(eventTime);
        return new TradeEvent(
                UUIDv7.generate(), t, t, "test", BTC_USDT, 1L, 1,
                new BigDecimal(price), new BigDecimal(size), side, "t-" + System.nanoTime());
    }
}
