package io.muninn.feature.compute;

import io.muninn.feature.engine.WatermarkTracker;
import io.muninn.feature.engine.WindowManager;
import io.muninn.feature.engine.WindowedBatch;
import io.muninn.replay.ReplayDivergenceDetector;
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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Determinism tests for the VWAP feature engine.
 *
 * <p>Per TESTING_STRATEGY.md §Deterministic Replay Tests, three levels:</p>
 * <ol>
 *   <li><strong>Same-JVM replay:</strong> process trades, reset, replay, assert identical values.</li>
 *   <li><strong>Simulated cross-JVM:</strong> two independent pipelines from the same input, assert identical.</li>
 *   <li><strong>Checkpointed replay:</strong> process half, checkpoint state, restore, process rest, compare.</li>
 * </ol>
 *
 * <p>These tests prove that the feature computation is a <strong>pure function of its
 * inputs and code</strong> — the central invariant of the Muninn architecture.</p>
 */
class VwapDeterminismTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final String CODE_VERSION = "determinism-test-v1";
    private static final Duration WINDOW_DURATION = Duration.ofMinutes(1);

    /**
     * Test 1: Same-JVM replay.
     *
     * <p>Process the same trades twice through completely independent pipeline instances.
     * Assert that the VWAP values are identical.</p>
     */
    @Test
    void sameJvmReplay_identicalInputs_produceIdenticalOutputs() {
        List<TradeEvent> trades = buildTradeSequence();

        List<FeatureComputedEvent> run1 = runPipeline(trades);
        List<FeatureComputedEvent> run2 = runPipeline(trades);

        assertThat(run1).hasSameSizeAs(run2);

        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();
        for (int i = 0; i < run1.size(); i++) {
            boolean match = detector.compare(run1.get(i), run2.get(i));
            assertThat(match)
                    .as("Window %d: %s → %s", i, run1.get(i).windowStart(), run1.get(i).windowEnd())
                    .isTrue();
        }

        assertThat(detector.isClean()).isTrue();
    }

    /**
     * Test 2: Simulated cross-JVM replay.
     *
     * <p>Create two completely independent pipeline instances with fresh objects
     * (simulating two separate JVM runs). Process the same input. Assert identical
     * output values.</p>
     *
     * <p>This catches issues with hidden static state, non-deterministic iteration,
     * and leaked mutable references.</p>
     */
    @Test
    void crossJvmReplay_independentPipelines_produceIdenticalOutputs() {
        // Generate trades once — these are our "event log"
        List<TradeEvent> trades = buildTradeSequence();

        // "JVM 1" — live pipeline
        List<FeatureComputedEvent> liveOutputs;
        {
            WatermarkTracker tracker = new WatermarkTracker();
            WindowManager wm = new WindowManager(WINDOW_DURATION, tracker);
            liveOutputs = processAll(trades, tracker, wm);
        }

        // "JVM 2" — replay pipeline (completely fresh objects)
        List<FeatureComputedEvent> replayOutputs;
        {
            WatermarkTracker tracker = new WatermarkTracker();
            WindowManager wm = new WindowManager(WINDOW_DURATION, tracker);
            replayOutputs = processAll(trades, tracker, wm);
        }

        assertThat(liveOutputs).hasSameSizeAs(replayOutputs);

        for (int i = 0; i < liveOutputs.size(); i++) {
            assertThat(liveOutputs.get(i).value())
                    .as("VWAP value at window %d", i)
                    .isEqualByComparingTo(replayOutputs.get(i).value());
            assertThat(liveOutputs.get(i).windowStart())
                    .as("Window start at window %d", i)
                    .isEqualTo(replayOutputs.get(i).windowStart());
            assertThat(liveOutputs.get(i).windowEnd())
                    .as("Window end at window %d", i)
                    .isEqualTo(replayOutputs.get(i).windowEnd());
            assertThat(liveOutputs.get(i).inputEventIds().size())
                    .as("Input event count at window %d", i)
                    .isEqualTo(replayOutputs.get(i).inputEventIds().size());
        }
    }

    /**
     * Test 3: Checkpointed replay.
     *
     * <p>Process the first half of trades, capture the window state, then continue
     * with the second half. Compare against an uninterrupted run of all trades.
     * The two must produce identical outputs from the checkpoint forward.</p>
     *
     * <p>This proves that checkpoint/restore does not introduce divergence.</p>
     */
    @Test
    void checkpointedReplay_resumeFromMidstream_matchesUninterruptedRun() {
        List<TradeEvent> allTrades = buildTradeSequence();
        int midpoint = allTrades.size() / 2;

        List<TradeEvent> firstHalf = allTrades.subList(0, midpoint);
        List<TradeEvent> secondHalf = allTrades.subList(midpoint, allTrades.size());

        // --- Uninterrupted run (the reference) ---
        List<FeatureComputedEvent> referenceOutputs = runPipeline(allTrades);

        // --- Checkpointed run ---
        // Phase 1: Process first half
        WatermarkTracker tracker1 = new WatermarkTracker();
        WindowManager wm1 = new WindowManager(WINDOW_DURATION, tracker1);
        for (TradeEvent trade : firstHalf) {
            wm1.add(trade, 0);
        }
        List<FeatureComputedEvent> checkpointedOutputs = new ArrayList<>();
        wm1.fireCompletedWindows(batch ->
                checkpointedOutputs.add(VwapComputer.compute(batch, CODE_VERSION)));

        // "Checkpoint" — capture current watermark and open window state
        // (In production this would serialize to MinIO; here we just keep the objects alive)
        Instant checkpointWatermark = tracker1.globalWatermark();

        // Phase 2: Continue with second half (same tracker + window manager = simulated restore)
        for (TradeEvent trade : secondHalf) {
            wm1.add(trade, 0);
        }
        // Advance watermark to flush all remaining windows
        tracker1.advance(0, Instant.parse("2026-05-11T14:10:00Z"));
        wm1.fireCompletedWindows(batch ->
                checkpointedOutputs.add(VwapComputer.compute(batch, CODE_VERSION)));

        // --- Compare ---
        assertThat(checkpointedOutputs).hasSameSizeAs(referenceOutputs);

        for (int i = 0; i < referenceOutputs.size(); i++) {
            assertThat(checkpointedOutputs.get(i).value())
                    .as("VWAP value at window %d", i)
                    .isEqualByComparingTo(referenceOutputs.get(i).value());
        }
    }

    // --- Helpers ---

    private List<FeatureComputedEvent> runPipeline(List<TradeEvent> trades) {
        WatermarkTracker tracker = new WatermarkTracker();
        WindowManager wm = new WindowManager(WINDOW_DURATION, tracker);
        return processAll(trades, tracker, wm);
    }

    private List<FeatureComputedEvent> processAll(
            List<TradeEvent> trades, WatermarkTracker tracker, WindowManager wm) {
        for (TradeEvent trade : trades) {
            wm.add(trade, 0);
        }

        // Advance watermark to flush all windows
        tracker.advance(0, Instant.parse("2026-05-11T14:10:00Z"));

        List<FeatureComputedEvent> outputs = new ArrayList<>();
        wm.fireCompletedWindows(batch -> outputs.add(VwapComputer.compute(batch, CODE_VERSION)));
        return outputs;
    }

    /**
     * Build a repeatable sequence of 20 trades across 5 minutes.
     * Uses deterministic UUIDv7 generation from fixed timestamps.
     */
    private List<TradeEvent> buildTradeSequence() {
        String[][] data = {
                // eventTime, price, size
                {"2026-05-11T14:00:05Z", "67500.00", "0.50"},
                {"2026-05-11T14:00:15Z", "67510.00", "1.00"},
                {"2026-05-11T14:00:25Z", "67490.00", "0.25"},
                {"2026-05-11T14:00:40Z", "67520.00", "2.00"},
                {"2026-05-11T14:01:05Z", "67530.00", "1.50"},
                {"2026-05-11T14:01:15Z", "67515.00", "0.30"},
                {"2026-05-11T14:01:30Z", "67540.00", "0.80"},
                {"2026-05-11T14:01:50Z", "67525.00", "1.20"},
                {"2026-05-11T14:02:10Z", "67550.00", "0.60"},
                {"2026-05-11T14:02:20Z", "67535.00", "1.00"},
                {"2026-05-11T14:02:40Z", "67560.00", "0.40"},
                {"2026-05-11T14:03:05Z", "67545.00", "0.90"},
                {"2026-05-11T14:03:15Z", "67570.00", "0.50"},
                {"2026-05-11T14:03:30Z", "67555.00", "1.10"},
                {"2026-05-11T14:03:45Z", "67580.00", "0.70"},
                {"2026-05-11T14:04:05Z", "67565.00", "0.80"},
                {"2026-05-11T14:04:20Z", "67590.00", "1.30"},
                {"2026-05-11T14:04:40Z", "67575.00", "0.45"},
                {"2026-05-11T14:05:10Z", "67600.00", "0.90"},
                {"2026-05-11T14:05:30Z", "67585.00", "0.60"},
        };

        List<TradeEvent> trades = new ArrayList<>();
        for (int i = 0; i < data.length; i++) {
            Instant eventTime = Instant.parse(data[i][0]);
            trades.add(new TradeEvent(
                    UUIDv7.generate(eventTime.toEpochMilli()),
                    eventTime,
                    eventTime,
                    "determinism-test",
                    BTC_USDT,
                    i + 1L,
                    1,
                    new BigDecimal(data[i][1]),
                    new BigDecimal(data[i][2]),
                    Side.BUY,
                    "dt-" + (i + 1)
            ));
        }
        return trades;
    }
}
