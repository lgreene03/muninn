package io.muninn.feature.engine;

import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link WindowManager}.
 */
class WindowManagerTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    private WatermarkTracker watermarkTracker;
    private WindowManager windowManager;

    @BeforeEach
    void setUp() {
        watermarkTracker = new WatermarkTracker();
        windowManager = new WindowManager(ONE_MINUTE, watermarkTracker);
    }

    @Test
    void add_singleTrade_bufferedInWindow() {
        TradeEvent trade = trade("2026-05-11T14:00:30Z");

        boolean accepted = windowManager.add(trade, 0);

        assertThat(accepted).isTrue();
        assertThat(windowManager.openWindowCount()).isEqualTo(1);
        assertThat(windowManager.bufferedEventCount()).isEqualTo(1);
    }

    @Test
    void add_tradesInSameWindow_bufferedTogether() {
        windowManager.add(trade("2026-05-11T14:00:10Z"), 0);
        windowManager.add(trade("2026-05-11T14:00:20Z"), 0);
        windowManager.add(trade("2026-05-11T14:00:30Z"), 0);

        assertThat(windowManager.openWindowCount()).isEqualTo(1);
        assertThat(windowManager.bufferedEventCount()).isEqualTo(3);
    }

    @Test
    void add_tradesInDifferentWindows_separateBuffers() {
        windowManager.add(trade("2026-05-11T14:00:30Z"), 0);
        windowManager.add(trade("2026-05-11T14:01:30Z"), 0);

        assertThat(windowManager.openWindowCount()).isEqualTo(2);
    }

    @Test
    void fireCompletedWindows_windowNotComplete_noFiring() {
        windowManager.add(trade("2026-05-11T14:00:30Z"), 0);

        List<WindowedBatch> fired = new ArrayList<>();
        int count = windowManager.fireCompletedWindows(fired::add);

        assertThat(count).isZero();
        assertThat(fired).isEmpty();
    }

    @Test
    void fireCompletedWindows_watermarkAdvancesPastWindow_fires() {
        // Add trades in 14:00 window
        windowManager.add(trade("2026-05-11T14:00:10Z"), 0);
        windowManager.add(trade("2026-05-11T14:00:20Z"), 0);

        // Advance watermark past 14:01 by adding a trade at 14:01:30
        windowManager.add(trade("2026-05-11T14:01:30Z"), 0);

        List<WindowedBatch> fired = new ArrayList<>();
        int count = windowManager.fireCompletedWindows(fired::add);

        assertThat(count).isEqualTo(1);
        assertThat(fired).hasSize(1);
        assertThat(fired.getFirst().windowStart()).isEqualTo(Instant.parse("2026-05-11T14:00:00Z"));
        assertThat(fired.getFirst().windowEnd()).isEqualTo(Instant.parse("2026-05-11T14:01:00Z"));
        assertThat(fired.getFirst().trades()).hasSize(2);
    }

    @Test
    void fireCompletedWindows_multipleWindowsFire_inOrder() {
        // Fill three 1-minute windows
        windowManager.add(trade("2026-05-11T14:00:10Z"), 0);
        windowManager.add(trade("2026-05-11T14:01:10Z"), 0);
        windowManager.add(trade("2026-05-11T14:02:10Z"), 0);
        // Advance watermark past all three
        windowManager.add(trade("2026-05-11T14:03:10Z"), 0);

        List<WindowedBatch> fired = new ArrayList<>();
        windowManager.fireCompletedWindows(fired::add);

        assertThat(fired).hasSize(3);
        assertThat(fired.get(0).windowStart()).isEqualTo(Instant.parse("2026-05-11T14:00:00Z"));
        assertThat(fired.get(1).windowStart()).isEqualTo(Instant.parse("2026-05-11T14:01:00Z"));
        assertThat(fired.get(2).windowStart()).isEqualTo(Instant.parse("2026-05-11T14:02:00Z"));
    }

    @Test
    void fireCompletedWindows_firedWindowsRemovedFromBuffer() {
        windowManager.add(trade("2026-05-11T14:00:10Z"), 0);
        windowManager.add(trade("2026-05-11T14:01:10Z"), 0);

        List<WindowedBatch> fired = new ArrayList<>();
        windowManager.fireCompletedWindows(fired::add);

        // Only 14:00 window fired (watermark is at 14:01:10, window end 14:01 is complete)
        assertThat(fired).hasSize(1);
        assertThat(windowManager.openWindowCount()).isEqualTo(1); // 14:01 window still open
    }

    @Test
    void add_lateEvent_isDropped() {
        // Process two windows normally
        windowManager.add(trade("2026-05-11T14:00:10Z"), 0);
        windowManager.add(trade("2026-05-11T14:01:10Z"), 0);

        // Fire the first window
        windowManager.fireCompletedWindows(batch -> {});

        // Now try to add an event from the already-closed window
        boolean accepted = windowManager.add(trade("2026-05-11T14:00:05Z"), 0);

        assertThat(accepted).isFalse();
    }

    @Test
    void multiPartition_globalWatermarkIsMinimum() {
        // Partition 0 is ahead, partition 1 is behind
        windowManager.add(trade("2026-05-11T14:02:00Z"), 0); // partition 0
        windowManager.add(trade("2026-05-11T14:00:30Z"), 1); // partition 1

        // Global watermark = min(14:02:00, 14:00:30) = 14:00:30
        // No windows should fire because watermark hasn't passed any window end
        List<WindowedBatch> fired = new ArrayList<>();
        windowManager.fireCompletedWindows(fired::add);

        assertThat(fired).isEmpty();

        // Now advance partition 1 past 14:01
        windowManager.add(trade("2026-05-11T14:01:10Z"), 1);

        // Global watermark = min(14:02:00, 14:01:10) = 14:01:10
        // The 14:00 window (end 14:01:00) should now fire
        windowManager.fireCompletedWindows(fired::add);

        assertThat(fired).hasSize(1);
        assertThat(fired.getFirst().windowStart()).isEqualTo(Instant.parse("2026-05-11T14:00:00Z"));
    }

    // --- Helper ---

    private TradeEvent trade(String eventTime) {
        return new TradeEvent(
                UUIDv7.generate(),
                Instant.parse(eventTime),
                Instant.parse(eventTime),
                "test",
                BTC_USDT,
                1L, 1,
                new BigDecimal("67500.00"),
                new BigDecimal("1.0"),
                Side.BUY,
                "t-" + System.nanoTime()
        );
    }
}
