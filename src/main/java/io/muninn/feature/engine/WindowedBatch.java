package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;

import java.time.Instant;
import java.util.List;

/**
 * An immutable batch of events belonging to a single tumbling window.
 *
 * <p>Passed to a {@link io.muninn.feature.compute.VwapComputer} (or any feature computer)
 * for deterministic computation. The window boundaries are event-time-aligned,
 * not wall-clock-aligned.</p>
 *
 * @param windowStart the inclusive start of the window (epoch-aligned)
 * @param windowEnd   the exclusive end of the window
 * @param trades      the trade events in this window, in event-time order
 */
public record WindowedBatch(
        Instant windowStart,
        Instant windowEnd,
        List<TradeEvent> trades
) {

    public WindowedBatch {
        if (windowStart == null) throw new IllegalArgumentException("windowStart is required");
        if (windowEnd == null) throw new IllegalArgumentException("windowEnd is required");
        if (!windowStart.isBefore(windowEnd)) throw new IllegalArgumentException("windowStart must be before windowEnd");
        if (trades == null) throw new IllegalArgumentException("trades is required");
        trades = List.copyOf(trades);
    }

    /**
     * @return true if this window contains no trades
     */
    public boolean isEmpty() {
        return trades.isEmpty();
    }

    /**
     * @return the number of trades in this window
     */
    public int size() {
        return trades.size();
    }
}
