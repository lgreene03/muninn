package io.muninn.replay;

import java.time.Duration;

public record ReplayResult(long eventsReplayed, Duration elapsed) {
}
