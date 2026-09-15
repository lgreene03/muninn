package io.muninn.feature.microstructure;

import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link VPINComputer}, focused on the four defects fixed relative to
 * the original implementation (see class Javadoc and the P2 roadmap):
 * <ol>
 *   <li>Bucket carryover — an overshooting trade is split, not discarded.</li>
 *   <li>No mutable instance state, and state is threaded explicitly (no field on the
 *       class carries anything between calls — every test below constructs a fresh
 *       {@code BucketState} and passes it in).</li>
 *   <li>{@link BigDecimal} arithmetic throughout — no {@code double}.</li>
 *   <li>No {@code NaN} — readiness is carried by {@code bucketsFilled} via
 *       {@link VPINComputer.BucketState#bucketsFilled()}, and {@link VPINComputer.Result#event()}
 *       is empty rather than a sentinel value until enough buckets have filled.</li>
 * </ol>
 */
class VPINComputerTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instant WINDOW_START = Instant.parse("2026-05-11T14:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-05-11T14:01:00Z");
    private static final String CODE_VERSION = "test-sha-001";

    @Test
    void compute_tradeExactlyFillsBucket_noLeftoverAndFullyToxic() {
        // One BUY trade of exactly the bucket size, one-bucket moving average.
        WindowedBatch batch = batchOf(trade("10", Side.BUY, "2026-05-11T14:00:10Z"));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        assertThat(result.event()).isPresent();
        // imbalance = |10 - 0| = 10; vpin = 10 / (1 * 10) = 1.0 (maximally toxic — all one side)
        assertThat(result.event().get().value()).isEqualByComparingTo("1.00000000");
        assertThat(result.nextState().currentBucketBuyVolume()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(result.nextState().currentBucketSellVolume()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(result.nextState().bucketsFilled()).isEqualTo(1);
    }

    @Test
    void compute_tradeOvershootsBucketBoundary_splitsRatherThanDiscardingExcess() {
        // DEFECT 1 — bucket carryover. bucketVolumeSize=10; a single 15-unit BUY trade
        // must fill the first bucket to EXACTLY 10 (recording imbalance=10) and carry
        // the remaining 5 into the next (in-progress) bucket. The old implementation
        // discarded the 5 entirely on "reset" — this proves nothing is lost: the
        // leftover plus the amount consumed by the completed bucket sums back to the
        // trade's full size.
        WindowedBatch batch = batchOf(trade("15", Side.BUY, "2026-05-11T14:00:10Z"));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        assertThat(result.event()).isPresent();
        assertThat(result.event().get().value()).isEqualByComparingTo("1.00000000");
        assertThat(result.nextState().bucketsFilled()).isEqualTo(1);
        // The excess volume is carried forward, not discarded.
        assertThat(result.nextState().currentBucketBuyVolume())
                .as("overshoot must carry into the next bucket rather than vanish")
                .isEqualByComparingTo("5");
        assertThat(result.nextState().currentBucketSellVolume()).isEqualByComparingTo(BigDecimal.ZERO);

        BigDecimal consumedByCompletedBucket = new BigDecimal("10"); // the one recorded imbalance's bucket volume
        BigDecimal totalAccountedFor = result.nextState().currentBucketBuyVolume().add(consumedByCompletedBucket);
        assertThat(totalAccountedFor)
                .as("no volume may be discarded: leftover + completed-bucket volume == trade size")
                .isEqualByComparingTo("15");
    }

    @Test
    void compute_hugeTradeSpansMultipleBuckets_splitsAcrossAllOfThem() {
        // A single 25-unit trade against a bucket size of 10 must fill TWO buckets
        // completely and leave 5 in progress — proving the carryover loop handles more
        // than one boundary crossing, not just a single overshoot.
        WindowedBatch batch = batchOf(trade("25", Side.BUY, "2026-05-11T14:00:10Z"));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 3, CODE_VERSION);

        // Only 2 of the 3 required buckets have filled — not ready yet, no NaN, just empty.
        assertThat(result.event()).isEmpty();
        assertThat(result.nextState().bucketsFilled()).isEqualTo(2);
        assertThat(result.nextState().currentBucketBuyVolume()).isEqualByComparingTo("5");
    }

    @Test
    void compute_fewerThanNumberOfBuckets_returnsEmptyNotNaN() {
        // DEFECT 4 — no NaN. With numberOfBuckets=5 and only one small trade, VPIN is
        // not ready; the old code returned Double.NaN, this returns Optional.empty().
        WindowedBatch batch = batchOf(trade("1", Side.BUY, "2026-05-11T14:00:10Z"));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 5, CODE_VERSION);

        assertThat(result.event()).isEmpty();
        assertThat(result.nextState().bucketsFilled()).isZero();
    }

    @Test
    void compute_windowWithNoTrades_isNoOpAndReturnsEmpty() {
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of());

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        assertThat(result.event()).isEmpty();
        assertThat(result.nextState()).isEqualTo(VPINComputer.BucketState.EMPTY);
        assertThat(result.unknownSideVolumeProcessed()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    @Test
    void compute_perfectlyBalancedBuyAndSell_vpinIsZero() {
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(
                trade("5", Side.BUY, "2026-05-11T14:00:10Z"),
                trade("5", Side.SELL, "2026-05-11T14:00:20Z")
        ));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        assertThat(result.event()).isPresent();
        assertThat(result.event().get().value()).isEqualByComparingTo("0.00000000");
    }

    @Test
    void compute_unknownSide_splitsFiftyFiftyAndReportsFullVolumeProcessed() {
        // DEFECT 5 (softer) — the 50/50 fallback is kept, but its volume must be
        // observable so a caller can raise a counter on it rather than pass silently.
        WindowedBatch batch = batchOf(trade("10", Side.UNKNOWN, "2026-05-11T14:00:10Z"));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        assertThat(result.event()).isPresent();
        // Split 5/5 -> imbalance 0, perfectly "balanced" by construction of the fallback.
        assertThat(result.event().get().value()).isEqualByComparingTo("0.00000000");
        assertThat(result.unknownSideVolumeProcessed())
                .as("the full trade size must be reported, not the half applied to each side")
                .isEqualByComparingTo("10");
    }

    @Test
    void compute_isPureFunction_sameStateAndBatchProduceIdenticalResult() {
        VPINComputer.BucketState state = new VPINComputer.BucketState(
                new BigDecimal("3"), new BigDecimal("1"), List.of(new BigDecimal("2")));
        WindowedBatch batch = batchOf(trade("4", Side.BUY, "2026-05-11T14:00:10Z"));

        VPINComputer.Result r1 = VPINComputer.compute(state, batch, new BigDecimal("10"), 2, CODE_VERSION);
        VPINComputer.Result r2 = VPINComputer.compute(state, batch, new BigDecimal("10"), 2, CODE_VERSION);

        assertThat(r1.nextState()).isEqualTo(r2.nextState());
        assertThat(r1.event().isPresent()).isEqualTo(r2.event().isPresent());
        r1.event().ifPresent(e1 -> assertThat(e1.value()).isEqualByComparingTo(r2.event().orElseThrow().value()));
        // The state instance passed in is immutable and untouched by the call.
        assertThat(state.currentBucketBuyVolume()).isEqualByComparingTo("3");
        assertThat(state.completedBucketImbalances()).containsExactly(new BigDecimal("2"));
    }

    @Test
    void compute_bucketHistoryBounded_oldestImbalanceDropped() {
        // numberOfBuckets=2, three separate full buckets fed as three trades in one
        // call — only the most recent two imbalances should survive.
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(
                trade("10", Side.BUY, "2026-05-11T14:00:01Z"),  // bucket 1: imbalance 10
                trade("10", Side.SELL, "2026-05-11T14:00:02Z"), // bucket 2: imbalance 10 (but SELL-only)
                trade("6", Side.BUY, "2026-05-11T14:00:03Z"),
                trade("4", Side.SELL, "2026-05-11T14:00:04Z")   // bucket 3: imbalance |6-4| = 2
        ));

        VPINComputer.Result result = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 2, CODE_VERSION);

        assertThat(result.nextState().bucketsFilled()).isEqualTo(2);
        assertThat(result.nextState().completedBucketImbalances())
                .as("only the two most recent buckets are kept, oldest dropped")
                .containsExactly(new BigDecimal("10"), new BigDecimal("2"));
        // vpin = (10 + 2) / (2 * 10) = 0.60000000
        assertThat(result.event()).isPresent();
        assertThat(result.event().get().value()).isEqualByComparingTo("0.60000000");
    }

    @Test
    void compute_stateCarriesForwardAcrossCalls_toEventualReadiness() {
        // Simulates two successive windows: the first leaves a bucket in progress,
        // the second finishes it and reaches readiness.
        WindowedBatch window1 = batchOf(trade("7", Side.BUY, "2026-05-11T14:00:10Z"));
        VPINComputer.Result r1 = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, window1, new BigDecimal("10"), 1, CODE_VERSION);
        assertThat(r1.event()).isEmpty();
        assertThat(r1.nextState().currentBucketBuyVolume()).isEqualByComparingTo("7");

        WindowedBatch window2 = new WindowedBatch(
                Instant.parse("2026-05-11T14:01:00Z"), Instant.parse("2026-05-11T14:02:00Z"),
                List.of(trade("5", Side.BUY, "2026-05-11T14:01:10Z")));
        VPINComputer.Result r2 = VPINComputer.compute(
                r1.nextState(), window2, new BigDecimal("10"), 1, CODE_VERSION);

        // 7 (carried) + 5 (new) = 12 -> fills the bucket (10) with 2 left over.
        assertThat(r2.event()).isPresent();
        assertThat(r2.event().get().value()).isEqualByComparingTo("1.00000000");
        assertThat(r2.nextState().currentBucketBuyVolume()).isEqualByComparingTo("2");
    }

    @Test
    void compute_negativeOrZeroBucketVolumeSize_throws() {
        WindowedBatch batch = batchOf(trade("1", Side.BUY, "2026-05-11T14:00:10Z"));

        assertThatThrownBy(() -> VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, BigDecimal.ZERO, 1, CODE_VERSION))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void compute_nonPositiveNumberOfBuckets_throws() {
        WindowedBatch batch = batchOf(trade("1", Side.BUY, "2026-05-11T14:00:10Z"));

        assertThatThrownBy(() -> VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 0, CODE_VERSION))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void compute_nullState_treatedAsEmpty() {
        WindowedBatch batch = batchOf(trade("10", Side.BUY, "2026-05-11T14:00:10Z"));

        VPINComputer.Result withNull = VPINComputer.compute(null, batch, new BigDecimal("10"), 1, CODE_VERSION);
        VPINComputer.Result withEmpty = VPINComputer.compute(
                VPINComputer.BucketState.EMPTY, batch, new BigDecimal("10"), 1, CODE_VERSION);

        assertThat(withNull.nextState()).isEqualTo(withEmpty.nextState());
    }

    // --- Helpers ---

    private WindowedBatch batchOf(TradeEvent trade) {
        return new WindowedBatch(WINDOW_START, WINDOW_END, List.of(trade));
    }

    private TradeEvent trade(String size, Side side, String eventTime) {
        Instant t = Instant.parse(eventTime);
        return new TradeEvent(
                UUIDv7.generate(), t, t, "test", BTC_USDT, 1L, 1,
                new BigDecimal("100.00"), new BigDecimal(size), side, "t-" + System.nanoTime());
    }
}
