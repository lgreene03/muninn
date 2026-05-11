package io.muninn.feature.engine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link WatermarkTracker}.
 */
class WatermarkTrackerTest {

    private WatermarkTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new WatermarkTracker();
    }

    @Test
    void globalWatermark_noPartitions_returnsMin() {
        assertThat(tracker.globalWatermark()).isEqualTo(Instant.MIN);
    }

    @Test
    void advance_singlePartition_updatesGlobalWatermark() {
        Instant t1 = Instant.parse("2026-05-11T14:00:00Z");

        boolean advanced = tracker.advance(0, t1);

        assertThat(advanced).isTrue();
        assertThat(tracker.globalWatermark()).isEqualTo(t1);
        assertThat(tracker.partitionWatermark(0)).isEqualTo(t1);
    }

    @Test
    void advance_isMonotonic_ignoresOlderTimestamps() {
        Instant t1 = Instant.parse("2026-05-11T14:00:10Z");
        Instant t2 = Instant.parse("2026-05-11T14:00:05Z"); // older

        tracker.advance(0, t1);
        boolean advanced = tracker.advance(0, t2);

        assertThat(advanced).isFalse();
        assertThat(tracker.partitionWatermark(0)).isEqualTo(t1); // unchanged
    }

    @Test
    void globalWatermark_multiplePartitions_returnsMinimum() {
        Instant t1 = Instant.parse("2026-05-11T14:00:10Z");
        Instant t2 = Instant.parse("2026-05-11T14:00:05Z");

        tracker.advance(0, t1);
        tracker.advance(1, t2);

        // Global = min(14:00:10, 14:00:05) = 14:00:05
        assertThat(tracker.globalWatermark()).isEqualTo(t2);
    }

    @Test
    void isWindowComplete_beforeWatermark_returnsTrue() {
        tracker.advance(0, Instant.parse("2026-05-11T14:02:00Z"));

        // Window ending at 14:01:00 is before the watermark at 14:02:00
        assertThat(tracker.isWindowComplete(Instant.parse("2026-05-11T14:01:00Z"))).isTrue();
    }

    @Test
    void isWindowComplete_afterWatermark_returnsFalse() {
        tracker.advance(0, Instant.parse("2026-05-11T14:01:00Z"));

        // Window ending at 14:02:00 is after the watermark at 14:01:00
        assertThat(tracker.isWindowComplete(Instant.parse("2026-05-11T14:02:00Z"))).isFalse();
    }

    @Test
    void reset_clearsAllPartitions() {
        tracker.advance(0, Instant.parse("2026-05-11T14:00:00Z"));
        tracker.advance(1, Instant.parse("2026-05-11T14:00:00Z"));

        tracker.reset();

        assertThat(tracker.partitionCount()).isZero();
        assertThat(tracker.globalWatermark()).isEqualTo(Instant.MIN);
    }
}
