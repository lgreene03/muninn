package io.muninn.feature.engine;

import io.muninn.feature.engine.TumblingWindowAssigner.WindowBounds;
import io.muninn.shared.event.MarketEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

/**
 * Manages tumbling windows: buffers incoming market events, detects when windows
 * are complete (via watermark advancement), and fires completed windows to a callback.
 *
 * <p>The window manager does NOT perform feature computation itself — it delegates
 * to a {@link Consumer} callback when a window closes. This separation keeps the
 * windowing logic testable independently of computation.</p>
 *
 * <p>The manager is generic over {@link MarketEvent}, not any single subtype: a window
 * may hold trades, order-book snapshots, or a mix of both, since {@link WindowedBatch}
 * is the shared unit of dispatch for every registered feature computer (VWAP, OBI,
 * micro-price, VPIN). The window manager assigns events to windows purely by event
 * time — it has no notion of "which feature this event is for" and must not gain one;
 * that decision belongs to each computer, which filters the batch it is handed.</p>
 *
 * <p>Windows are keyed by their start time. Events are assigned to windows using
 * {@link TumblingWindowAssigner}. A window fires when its end time is at or before
 * the global watermark from the {@link WatermarkTracker}.</p>
 *
 * <p>Thread-safety: intended for single-threaded use within the feature engine loop.
 * The internal map uses {@link ConcurrentSkipListMap} for sorted iteration, but
 * concurrent access is not the primary use case.</p>
 */
public final class WindowManager {

    private static final Logger log = LoggerFactory.getLogger(WindowManager.class);

    private final Duration windowDuration;
    private final WatermarkTracker watermarkTracker;
    private final NavigableMap<Instant, List<MarketEvent>> openWindows = new ConcurrentSkipListMap<>();

    // Metrics (nullable — for unit testing without a registry)
    private Counter lateEventCounter;

    public WindowManager(Duration windowDuration, WatermarkTracker watermarkTracker) {
        this(windowDuration, watermarkTracker, null);
    }

    public WindowManager(Duration windowDuration, WatermarkTracker watermarkTracker, MeterRegistry meterRegistry) {
        if (windowDuration == null || windowDuration.isZero() || windowDuration.isNegative()) {
            throw new IllegalArgumentException("windowDuration must be positive");
        }
        this.windowDuration = windowDuration;
        this.watermarkTracker = Objects.requireNonNull(watermarkTracker);

        if (meterRegistry != null) {
            // Tagged "engine" rather than a single feature name: this counter spans
            // every market event admitted to the shared window buffer, not one feature.
            this.lateEventCounter = Counter.builder("muninn.feature.late.events")
                    .tag("feature", "engine")
                    .tag("policy", "drop")
                    .register(meterRegistry);
        }
    }

    /**
     * Add a market event to the appropriate window.
     *
     * <p>If the event's time is at or before the global watermark, it is considered
     * late and is dropped (per the Phase 1 late-event policy).</p>
     *
     * @param event     the market event (trade, book snapshot, etc.)
     * @param partition the Kafka partition the event came from (for watermark tracking)
     * @return true if the event was accepted, false if it was late
     */
    public boolean add(MarketEvent event, int partition) {
        Instant eventTime = event.eventTime();

        // Advance watermark for this partition
        watermarkTracker.advance(partition, eventTime);

        // Check for late events
        Instant globalWatermark = watermarkTracker.globalWatermark();
        WindowBounds bounds = TumblingWindowAssigner.assign(eventTime, windowDuration);

        if (bounds.end().isBefore(globalWatermark) || bounds.end().equals(globalWatermark)) {
            // Late event — drop it
            if (lateEventCounter != null) lateEventCounter.increment();
            log.atWarn()
                    .addKeyValue("eventId", event.eventId())
                    .addKeyValue("eventTime", eventTime)
                    .addKeyValue("watermark", globalWatermark)
                    .addKeyValue("windowEnd", bounds.end())
                    .log("Late event dropped");
            return false;
        }

        // Add to the appropriate window buffer
        openWindows.computeIfAbsent(bounds.start(), k -> new ArrayList<>()).add(event);
        return true;
    }

    /**
     * Fire all windows that are now complete (windowEnd ≤ globalWatermark).
     *
     * <p>Completed windows are removed from the buffer and passed to the callback
     * as {@link WindowedBatch} instances. Windows are fired in chronological order.</p>
     *
     * @param callback receives each completed window batch
     * @return the number of windows fired
     */
    public int fireCompletedWindows(Consumer<WindowedBatch> callback) {
        Instant globalWatermark = watermarkTracker.globalWatermark();
        int fired = 0;

        // Iterate through windows in time order; fire all whose end ≤ watermark
        Iterator<Map.Entry<Instant, List<MarketEvent>>> it = openWindows.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Instant, List<MarketEvent>> entry = it.next();
            Instant windowStart = entry.getKey();
            Instant windowEnd = windowStart.plus(windowDuration);

            if (windowEnd.isBefore(globalWatermark) || windowEnd.equals(globalWatermark)) {
                List<MarketEvent> events = entry.getValue();
                // Sort by event time for deterministic ordering within the window
                events.sort(Comparator.comparing(MarketEvent::eventTime)
                        .thenComparing(MarketEvent::eventId));

                WindowedBatch batch = new WindowedBatch(windowStart, windowEnd, events);
                callback.accept(batch);
                it.remove();
                fired++;

                log.atDebug()
                        .addKeyValue("windowStart", windowStart)
                        .addKeyValue("windowEnd", windowEnd)
                        .addKeyValue("eventCount", events.size())
                        .log("Window fired");
            } else {
                break; // Sorted map — no more windows can be complete
            }
        }

        return fired;
    }

    /**
     * @return the number of currently open (buffered) windows
     */
    public int openWindowCount() {
        return openWindows.size();
    }

    /**
     * @return the total number of buffered events across all open windows
     */
    public int bufferedEventCount() {
        return openWindows.values().stream().mapToInt(List::size).sum();
    }

    public void reset() {
        openWindows.clear();
    }

    /**
     * @return the current global watermark
     */
    public Instant globalWatermark() {
        return watermarkTracker.globalWatermark();
    }

    /**
     * Seed the watermark for a partition during checkpoint restore. Delegates to the
     * underlying {@link WatermarkTracker#seed}; only intended for boot-time recovery.
     *
     * @param partition the partition number
     * @param watermark the watermark value to seed
     */
    public void seedWatermark(int partition, Instant watermark) {
        watermarkTracker.seed(partition, watermark);
    }

    /**
     * Restore buffered window state from a checkpoint snapshot.
     *
     * <p>Clears any existing buffered windows and re-seeds them from the checkpoint's
     * {@code windowStates}. This is the counterpart to {@link #snapshot()} and is used
     * on engine boot so partially-accumulated windows survive a restart (per
     * DETERMINISTIC_REPLAY.md §Checkpoints). The watermark itself is seeded separately
     * via {@link WatermarkTracker} so completed windows are not prematurely fired or
     * dropped as late during restore.</p>
     *
     * @param windowStates the open windows captured at checkpoint time
     * @return the number of windows restored
     */
    public int restore(List<io.muninn.feature.checkpoint.CheckpointState.WindowState> windowStates) {
        openWindows.clear();
        if (windowStates == null) {
            return 0;
        }
        for (io.muninn.feature.checkpoint.CheckpointState.WindowState ws : windowStates) {
            openWindows.put(ws.windowStart(), new ArrayList<>(ws.events()));
        }
        log.atInfo()
                .addKeyValue("restoredWindows", openWindows.size())
                .addKeyValue("restoredEvents", bufferedEventCount())
                .log("Window state restored from checkpoint");
        return openWindows.size();
    }

    /**
     * @return an immutable snapshot of all open windows and buffered events
     */
    public List<io.muninn.feature.checkpoint.CheckpointState.WindowState> snapshot() {
        List<io.muninn.feature.checkpoint.CheckpointState.WindowState> states = new ArrayList<>();
        for (Map.Entry<Instant, List<MarketEvent>> entry : openWindows.entrySet()) {
            Instant windowStart = entry.getKey();
            Instant windowEnd = windowStart.plus(windowDuration);
            states.add(new io.muninn.feature.checkpoint.CheckpointState.WindowState(
                    windowStart,
                    windowEnd,
                    entry.getValue()
            ));
        }
        return states;
    }
}
