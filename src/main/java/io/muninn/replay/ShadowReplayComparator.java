package io.muninn.replay;

import io.muninn.shared.event.FeatureComputedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Subscribes to both the live and replay output topics and feeds matched
 * pairs into the {@link ReplayDivergenceDetector}.
 *
 * <p>Pairs are keyed by {@code (featureName, featureVersion, windowStart)}.
 * The first arrival is buffered; the second arrival triggers the comparison
 * and clears the buffer entry. Unmatched entries are retained until eviction
 * (capacity-bounded) — they are not by themselves a divergence, because
 * replay may arrive minutes or hours after live.</p>
 *
 * <p>See {@code DETERMINISTIC_REPLAY.md} §Divergence Detection.</p>
 */
@Component
public class ShadowReplayComparator {

    private static final Logger log = LoggerFactory.getLogger(ShadowReplayComparator.class);

    /** Bound on buffered un-paired outputs to prevent unbounded memory growth. */
    private static final int BUFFER_CAPACITY = 10_000;

    private final ReplayDivergenceDetector detector;
    private final ConcurrentMap<WindowKey, FeatureComputedEvent> livePending = new ConcurrentHashMap<>();
    private final ConcurrentMap<WindowKey, FeatureComputedEvent> replayPending = new ConcurrentHashMap<>();

    public ShadowReplayComparator(ReplayDivergenceDetector detector) {
        this.detector = detector;
    }

    @KafkaListener(
            topics = "features.vwap.1m.v1",
            groupId = "muninn-shadow-comparator-live")
    public void onLive(FeatureComputedEvent live) {
        WindowKey key = WindowKey.of(live);
        FeatureComputedEvent replay = replayPending.remove(key);
        if (replay != null) {
            detector.compare(live, replay);
        } else {
            buffer(livePending, key, live, "live");
        }
    }

    @KafkaListener(
            topics = "features.vwap.1m.v1.replay",
            groupId = "muninn-shadow-comparator-replay")
    public void onReplay(FeatureComputedEvent replay) {
        WindowKey key = WindowKey.of(replay);
        FeatureComputedEvent live = livePending.remove(key);
        if (live != null) {
            detector.compare(live, replay);
        } else {
            buffer(replayPending, key, replay, "replay");
        }
    }

    private void buffer(ConcurrentMap<WindowKey, FeatureComputedEvent> pending,
                        WindowKey key, FeatureComputedEvent event, String side) {
        if (pending.size() >= BUFFER_CAPACITY) {
            log.atWarn()
                    .addKeyValue("side", side)
                    .addKeyValue("capacity", BUFFER_CAPACITY)
                    .log("Shadow comparator buffer at capacity; dropping oldest unpaired entries");
            pending.clear();
        }
        pending.put(key, event);
    }

    /** Test hook: how many outputs are awaiting their counterpart. */
    public int pendingPairs() {
        return livePending.size() + replayPending.size();
    }

    /**
     * Equivalence key for matching live and replay outputs. {@code windowStart}
     * is the natural identifier; feature name and version disambiguate when
     * multiple features share the topic family.
     */
    private record WindowKey(String featureName, String featureVersion, Instant windowStart) {
        static WindowKey of(FeatureComputedEvent e) {
            return new WindowKey(e.featureName(), e.featureVersion(), e.windowStart());
        }
    }
}
