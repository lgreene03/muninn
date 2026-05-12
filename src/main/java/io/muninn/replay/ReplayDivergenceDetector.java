package io.muninn.replay;

import io.muninn.shared.event.FeatureComputedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Compares live and replay feature outputs event-by-event to detect divergences.
 *
 * <p>A divergence occurs when the live and replay paths produce different values
 * for the same window. This is <strong>never</strong> expected — any divergence
 * indicates a determinism violation (per DETERMINISTIC_REPLAY.md §Divergence Detection).</p>
 *
 * <p>Emits the {@code muninn.replay.divergence.detected} metric on every mismatch.</p>
 */
@Component
public final class ReplayDivergenceDetector {

    private static final Logger log = LoggerFactory.getLogger(ReplayDivergenceDetector.class);

    private final Counter divergenceCounter;
    private final List<Divergence> divergences = new ArrayList<>();

    public ReplayDivergenceDetector(MeterRegistry meterRegistry) {
        this.divergenceCounter = Counter.builder("muninn.replay.divergence.detected")
                .tag("feature", "vwap.1m")
                .tag("version", "v1")
                .register(meterRegistry);
    }

    /**
     * For unit testing without a MeterRegistry.
     */
    public ReplayDivergenceDetector() {
        this.divergenceCounter = null;
    }

    /**
     * Compare a pair of live and replay outputs.
     *
     * @param live   the event from the live path
     * @param replay the event from the replay path
     * @return true if the events match, false if a divergence was detected
     */
    public boolean compare(FeatureComputedEvent live, FeatureComputedEvent replay) {
        boolean match = true;

        // Compare window boundaries
        if (!live.windowStart().equals(replay.windowStart())) {
            recordDivergence("windowStart", live.windowStart().toString(), replay.windowStart().toString());
            match = false;
        }
        if (!live.windowEnd().equals(replay.windowEnd())) {
            recordDivergence("windowEnd", live.windowEnd().toString(), replay.windowEnd().toString());
            match = false;
        }

        // Compare computed values (the critical check)
        if (live.value().compareTo(replay.value()) != 0) {
            recordDivergence("value", live.value().toPlainString(), replay.value().toPlainString());
            match = false;
        }

        // Compare provenance (input event count — IDs will differ due to UUIDv7 generation)
        if (live.inputEventIds().size() != replay.inputEventIds().size()) {
            recordDivergence("inputEventCount",
                    String.valueOf(live.inputEventIds().size()),
                    String.valueOf(replay.inputEventIds().size()));
            match = false;
        }

        if (match) {
            log.atDebug()
                    .addKeyValue("windowStart", live.windowStart())
                    .addKeyValue("windowEnd", live.windowEnd())
                    .addKeyValue("value", live.value())
                    .log("Live/replay match confirmed");
        }

        return match;
    }

    private void recordDivergence(String field, String liveValue, String replayValue) {
        Divergence d = new Divergence(field, liveValue, replayValue);
        divergences.add(d);

        if (divergenceCounter != null) {
            divergenceCounter.increment();
        }

        log.atError()
                .addKeyValue("field", field)
                .addKeyValue("liveValue", liveValue)
                .addKeyValue("replayValue", replayValue)
                .log("DIVERGENCE DETECTED — determinism violation");
    }

    /**
     * @return all detected divergences
     */
    public List<Divergence> divergences() {
        return List.copyOf(divergences);
    }

    /**
     * @return true if no divergences have been detected
     */
    public boolean isClean() {
        return divergences.isEmpty();
    }

    /**
     * A single field-level divergence between live and replay output.
     *
     * @param field       the field that diverged
     * @param liveValue   the value from the live path
     * @param replayValue the value from the replay path
     */
    public record Divergence(String field, String liveValue, String replayValue) {}
}
