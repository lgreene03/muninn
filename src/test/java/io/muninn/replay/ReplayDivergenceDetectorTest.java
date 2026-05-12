package io.muninn.replay;

import io.muninn.shared.event.FeatureComputedEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayDivergenceDetectorTest {

    private static final Instant WSTART = Instant.parse("2026-05-10T14:00:00Z");
    private static final Instant WEND   = Instant.parse("2026-05-10T14:01:00Z");

    @Test
    void compare_returnsTrueForIdenticalOutputs() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();

        FeatureComputedEvent live = output(new BigDecimal("60007.78"), 4);
        FeatureComputedEvent replay = output(new BigDecimal("60007.78"), 4);

        assertThat(detector.compare(live, replay)).isTrue();
        assertThat(detector.isClean()).isTrue();
        assertThat(detector.divergences()).isEmpty();
    }

    @Test
    void compare_detectsValueDivergence() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();

        FeatureComputedEvent live = output(new BigDecimal("60007.78"), 4);
        FeatureComputedEvent replay = output(new BigDecimal("60010.00"), 4);

        assertThat(detector.compare(live, replay)).isFalse();
        assertThat(detector.isClean()).isFalse();
        assertThat(detector.divergences())
                .extracting(ReplayDivergenceDetector.Divergence::field)
                .containsExactly("value");
    }

    @Test
    void compare_detectsWindowStartDivergence() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();

        FeatureComputedEvent live = output(new BigDecimal("100"), 1);
        FeatureComputedEvent replay = outputWith(WSTART.plusSeconds(60), WEND.plusSeconds(60),
                new BigDecimal("100"), 1);

        assertThat(detector.compare(live, replay)).isFalse();
        assertThat(detector.divergences())
                .extracting(ReplayDivergenceDetector.Divergence::field)
                .contains("windowStart", "windowEnd");
    }

    @Test
    void compare_detectsInputEventCountDivergence() {
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();

        FeatureComputedEvent live = output(new BigDecimal("100"), 4);
        FeatureComputedEvent replay = output(new BigDecimal("100"), 5);

        assertThat(detector.compare(live, replay)).isFalse();
        assertThat(detector.divergences())
                .extracting(ReplayDivergenceDetector.Divergence::field)
                .containsExactly("inputEventCount");
    }

    @Test
    void compare_valuesEqualUnderBigDecimalScale() {
        // 60007.78 and 60007.780 are numerically equal but textually different.
        // compareTo (used in the detector) must treat them as equal.
        ReplayDivergenceDetector detector = new ReplayDivergenceDetector();

        FeatureComputedEvent live = output(new BigDecimal("60007.78"), 4);
        FeatureComputedEvent replay = output(new BigDecimal("60007.780"), 4);

        assertThat(detector.compare(live, replay)).isTrue();
    }

    private FeatureComputedEvent output(BigDecimal value, int inputCount) {
        return outputWith(WSTART, WEND, value, inputCount);
    }

    private FeatureComputedEvent outputWith(Instant ws, Instant we, BigDecimal value, int inputCount) {
        List<UUID> inputs = java.util.stream.IntStream.range(0, inputCount)
                .mapToObj(i -> UUID.randomUUID())
                .toList();
        return new FeatureComputedEvent(
                UUID.randomUUID(),
                we,
                "vwap.1m",
                "v1",
                value,
                null,
                ws,
                we,
                inputs,
                "dev"
        );
    }
}
