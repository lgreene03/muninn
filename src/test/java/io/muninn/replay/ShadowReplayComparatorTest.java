package io.muninn.replay;

import io.muninn.shared.event.FeatureComputedEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ShadowReplayComparatorTest {

    @Test
    void liveArrivesFirst_thenReplay_triggersComparison() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();
        ShadowReplayComparator comparator = new ShadowReplayComparator(detector);

        Instant ws = Instant.parse("2026-05-10T14:00:00Z");
        FeatureComputedEvent live = output(ws, new BigDecimal("60007.78"));
        FeatureComputedEvent replay = output(ws, new BigDecimal("60007.78"));

        comparator.onLive(live);
        assertThat(comparator.pendingPairs()).isEqualTo(1);

        comparator.onReplay(replay);
        assertThat(comparator.pendingPairs()).isZero();
        assertThat(detector.isClean()).isTrue();
    }

    @Test
    void replayArrivesFirst_thenLive_triggersComparison() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();
        ShadowReplayComparator comparator = new ShadowReplayComparator(detector);

        Instant ws = Instant.parse("2026-05-10T14:00:00Z");
        FeatureComputedEvent replay = output(ws, new BigDecimal("60007.78"));
        FeatureComputedEvent live = output(ws, new BigDecimal("60007.78"));

        comparator.onReplay(replay);
        assertThat(comparator.pendingPairs()).isEqualTo(1);

        comparator.onLive(live);
        assertThat(comparator.pendingPairs()).isZero();
        assertThat(detector.isClean()).isTrue();
    }

    @Test
    void mismatchedValues_recordDivergence() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();
        ShadowReplayComparator comparator = new ShadowReplayComparator(detector);

        Instant ws = Instant.parse("2026-05-10T14:00:00Z");
        FeatureComputedEvent live = output(ws, new BigDecimal("60007.78"));
        FeatureComputedEvent replay = output(ws, new BigDecimal("60010.00"));

        comparator.onLive(live);
        comparator.onReplay(replay);

        assertThat(detector.isClean()).isFalse();
        assertThat(detector.divergences())
                .extracting(ReplayDivergenceDetector.Divergence::field)
                .containsExactly("value");
    }

    @Test
    void differentWindows_doNotPair() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();
        ShadowReplayComparator comparator = new ShadowReplayComparator(detector);

        Instant ws1 = Instant.parse("2026-05-10T14:00:00Z");
        Instant ws2 = Instant.parse("2026-05-10T14:01:00Z");

        comparator.onLive(output(ws1, new BigDecimal("100")));
        comparator.onReplay(output(ws2, new BigDecimal("100")));

        // Two unpaired events remain buffered; no comparison fired.
        assertThat(comparator.pendingPairs()).isEqualTo(2);
        assertThat(detector.isClean()).isTrue();
    }

    private FeatureComputedEvent output(Instant windowStart, BigDecimal value) {
        Instant windowEnd = windowStart.plusSeconds(60);
        return new FeatureComputedEvent(
                UUID.randomUUID(),
                windowEnd,
                "vwap.1m",
                "v1",
                value,
                null,
                windowStart,
                windowEnd,
                List.of(UUID.randomUUID()),
                "dev"
        );
    }
}
