package io.muninn.feature.engine;

import io.muninn.shared.event.FeatureComputedEvent;

import java.util.Optional;

/**
 * A feature computer that is dispatched once per closed {@link WindowedBatch}.
 *
 * <p>This is the shape {@link FeatureEngineRunner} dispatches over generically —
 * {@code VwapComputer}, {@code OrderBookImbalanceComputer}, and {@code MicroPriceComputer}
 * all fit it directly, since each is a pure function of "this window's batch" alone.
 * {@code VPINComputer} does not: VPIN's volume buckets do not align to time windows, so
 * it needs an explicit state parameter threaded across calls and is dispatched separately
 * by the runner rather than forced into this shape — see {@code VPINComputer}'s Javadoc.</p>
 *
 * <p>Implementations must return {@link Optional#empty()} rather than throwing or
 * fabricating a value when the batch holds none of the event type they need (e.g. an
 * OBI computer handed a window with only trades and no book snapshots).</p>
 */
@FunctionalInterface
public interface WindowFeatureComputer {

    /**
     * @param batch       the closed window's batch (may contain event types this
     *                    computer does not care about; it must filter for its own)
     * @param codeVersion the git SHA or version of the feature engine
     * @return the computed feature event, or empty if this window had nothing for
     *         this computer to act on
     */
    Optional<FeatureComputedEvent> compute(WindowedBatch batch, String codeVersion);
}
