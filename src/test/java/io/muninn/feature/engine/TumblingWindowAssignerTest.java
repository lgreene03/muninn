package io.muninn.feature.engine;

import io.muninn.feature.engine.TumblingWindowAssigner.WindowBounds;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link TumblingWindowAssigner}.
 */
class TumblingWindowAssignerTest {

    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    @Test
    void windowStart_midMinute_alignsToMinuteBoundary() {
        Instant eventTime = Instant.parse("2026-05-11T14:00:37.500Z");

        Instant start = TumblingWindowAssigner.windowStart(eventTime, ONE_MINUTE);

        assertThat(start).isEqualTo(Instant.parse("2026-05-11T14:00:00Z"));
    }

    @Test
    void windowStart_exactlyOnBoundary_alignsToThatBoundary() {
        Instant eventTime = Instant.parse("2026-05-11T14:01:00.000Z");

        Instant start = TumblingWindowAssigner.windowStart(eventTime, ONE_MINUTE);

        // An event at exactly 14:01:00 belongs to the 14:01 window
        assertThat(start).isEqualTo(Instant.parse("2026-05-11T14:01:00Z"));
    }

    @Test
    void windowEnd_isStartPlusDuration() {
        Instant eventTime = Instant.parse("2026-05-11T14:00:37.500Z");

        Instant end = TumblingWindowAssigner.windowEnd(eventTime, ONE_MINUTE);

        assertThat(end).isEqualTo(Instant.parse("2026-05-11T14:01:00Z"));
    }

    @Test
    void assign_returnsCorrectBounds() {
        Instant eventTime = Instant.parse("2026-05-11T14:02:45.123Z");

        WindowBounds bounds = TumblingWindowAssigner.assign(eventTime, ONE_MINUTE);

        assertThat(bounds.start()).isEqualTo(Instant.parse("2026-05-11T14:02:00Z"));
        assertThat(bounds.end()).isEqualTo(Instant.parse("2026-05-11T14:03:00Z"));
    }

    @Test
    void assign_fiveMinuteWindow_epochAligned() {
        Duration fiveMinutes = Duration.ofMinutes(5);
        Instant eventTime = Instant.parse("2026-05-11T14:07:30Z");

        WindowBounds bounds = TumblingWindowAssigner.assign(eventTime, fiveMinutes);

        assertThat(bounds.start()).isEqualTo(Instant.parse("2026-05-11T14:05:00Z"));
        assertThat(bounds.end()).isEqualTo(Instant.parse("2026-05-11T14:10:00Z"));
    }

    @Test
    void windowStart_zeroDuration_throwsIllegalArgument() {
        assertThatThrownBy(() -> TumblingWindowAssigner.windowStart(Instant.now(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }
}
