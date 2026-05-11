package io.muninn.replay;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link ReplayRequest}.
 */
class ReplayRequestTest {

    @Test
    void construct_validRequest_succeeds() {
        Instant from = Instant.parse("2026-01-01T00:00:00Z");
        Instant to = Instant.parse("2026-01-02T00:00:00Z");
        ReplayRequest req = new ReplayRequest("events.trade", from, to);
        assertThat(req.topic()).isEqualTo("events.trade");
        assertThat(req.from()).isEqualTo(from);
        assertThat(req.to()).isEqualTo(to);
    }

    @Test
    void construct_fromAfterTo_throwsIllegalArgument() {
        Instant from = Instant.parse("2026-01-02T00:00:00Z");
        Instant to = Instant.parse("2026-01-01T00:00:00Z");
        assertThatThrownBy(() -> new ReplayRequest("events.trade", from, to))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("from must be before to");
    }

    @Test
    void construct_blankTopic_throwsIllegalArgument() {
        assertThatThrownBy(() -> new ReplayRequest("", Instant.now(), Instant.now().plusSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic is required");
    }
}
