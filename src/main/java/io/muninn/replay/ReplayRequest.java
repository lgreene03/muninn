package io.muninn.replay;

import java.time.Instant;

public record ReplayRequest(
        String topic,
        Instant from,
        Instant to
) {

    public ReplayRequest {
        if (topic == null || topic.isBlank()) throw new IllegalArgumentException("topic is required");
        if (from == null) throw new IllegalArgumentException("from is required");
        if (to == null) throw new IllegalArgumentException("to is required");
        if (!from.isBefore(to)) throw new IllegalArgumentException("from must be before to");
    }
}
