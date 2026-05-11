package io.muninn.replay;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class ReplayRequestTest {

    @Test
    void validRequest() {
        Instant from = Instant.parse("2026-01-01T00:00:00Z");
        Instant to = Instant.parse("2026-01-02T00:00:00Z");
        ReplayRequest req = new ReplayRequest("trades", from, to);
        assertEquals("trades", req.topic());
    }

    @Test
    void rejectsFromAfterTo() {
        Instant from = Instant.parse("2026-01-02T00:00:00Z");
        Instant to = Instant.parse("2026-01-01T00:00:00Z");
        assertThrows(IllegalArgumentException.class, () -> new ReplayRequest("trades", from, to));
    }

    @Test
    void rejectsBlankTopic() {
        assertThrows(IllegalArgumentException.class, () ->
                new ReplayRequest("", Instant.now(), Instant.now().plusSeconds(60)));
    }
}
