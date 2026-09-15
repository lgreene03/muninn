package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;

import java.time.Instant;
import java.util.List;

/**
 * An immutable batch of events belonging to a single tumbling window.
 *
 * <p>Passed to a {@link io.muninn.feature.compute.VwapComputer} (or any other feature
 * computer) for deterministic computation. The window boundaries are event-time-aligned,
 * not wall-clock-aligned.</p>
 *
 * <p>A single window may contain a mix of {@link MarketEvent} subtypes — e.g. both
 * {@code TradeEvent}s and {@code OrderBookSnapshotEvent}s, since {@link WindowManager}
 * buffers every admitted market event in the same tumbling window regardless of its
 * concrete type (per DETERMINISTIC_REPLAY.md §How Live and Replay Share One Path — the
 * window manager does not know, and must not care, which feature will eventually read
 * a given event). Each feature computer is responsible for filtering {@link #events()}
 * down to the event type(s) it needs; see {@link #trades()} for the common case.</p>
 *
 * @param windowStart the inclusive start of the window (epoch-aligned)
 * @param windowEnd   the exclusive end of the window
 * @param events      every market event admitted into this window, in event-time order
 */
public record WindowedBatch(
        Instant windowStart,
        Instant windowEnd,
        List<MarketEvent> events
) {

    public WindowedBatch {
        if (windowStart == null) throw new IllegalArgumentException("windowStart is required");
        if (windowEnd == null) throw new IllegalArgumentException("windowEnd is required");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("windowStart must be before windowEnd");
        if (events == null) throw new IllegalArgumentException("events is required");
        events = List.copyOf(events);
    }

    /**
     * @return true if this window contains no events at all
     */
    public boolean isEmpty() {
        return events.isEmpty();
    }

    /**
     * @return the number of events (of any type) in this window
     */
    public int size() {
        return events.size();
    }

    /**
     * Convenience view over just the {@link TradeEvent}s in this window, in the same
     * relative order as {@link #events()}. Computers that only care about trades
     * (e.g. {@code VwapComputer}, {@code VPINComputer}) use this rather than filtering
     * {@link #events()} themselves.
     *
     * @return the trade events in this window; empty if the window held only
     *         non-trade events (e.g. a window with book snapshots but no trades)
     */
    public List<TradeEvent> trades() {
        return events.stream()
                .filter(TradeEvent.class::isInstance)
                .map(TradeEvent.class::cast)
                .toList();
    }
}
