package io.muninn.feature.microstructure;

import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pure-function Order Book Imbalance (OBI) computer.
 *
 * <p>OBI is a classic microstructure feature used to predict short-term price movements.
 * Formula: {@code (BidVolume - AskVolume) / (BidVolume + AskVolume)} over the top
 * {@code levelsToConsider} levels of the book. A value of +1.0 indicates extreme buy
 * pressure, -1.0 indicates extreme sell pressure.</p>
 *
 * <p>Uses {@link BigDecimal} arithmetic exclusively — floating-point is forbidden
 * per DETERMINISTIC_REPLAY.md §Anti-Patterns. This class has no state, no IO, no
 * clock reads; it is safe to call from both the live and replay paths, matching
 * {@code VwapComputer}'s discipline.</p>
 */
public final class OrderBookImbalanceComputer {

    /** Precision for intermediate calculations — matches {@code VwapComputer}. */
    private static final MathContext MATH_CONTEXT = new MathContext(18, RoundingMode.HALF_EVEN);

    /** Scale for the final OBI value. */
    private static final int RESULT_SCALE = 8;

    public static final String FEATURE_NAME = "obi";
    public static final String FEATURE_VERSION = "v1";

    private OrderBookImbalanceComputer() {
        // Utility class — no instantiation
    }

    /**
     * Compute OBI from the most recent order-book snapshot in the window.
     *
     * <p>Only the <strong>last</strong> snapshot in the batch (by event-time order)
     * contributes — OBI is a point-in-time measure of book state, not something that
     * aggregates across the window the way VWAP's volume sum does. If the window
     * holds no {@link OrderBookSnapshotEvent}, there is nothing to compute and this
     * returns {@link Optional#empty()} rather than fabricating a value (the classic
     * mistake the {@code microPrice != vwap} / degenerate-defaults tests guard against).</p>
     *
     * @param batch            the windowed batch (may also contain trades — only
     *                         book-snapshot events are read)
     * @param levelsToConsider how many levels of depth on each side to sum
     * @param codeVersion      the git SHA or version of the feature engine
     * @return the computed feature event, or empty if the window held no book snapshot
     * @throws IllegalArgumentException if {@code levelsToConsider} is not positive
     */
    public static Optional<FeatureComputedEvent> compute(
            WindowedBatch batch, int levelsToConsider, String codeVersion) {
        if (levelsToConsider <= 0) {
            throw new IllegalArgumentException("levelsToConsider must be positive");
        }
        if (codeVersion == null || codeVersion.isBlank()) {
            throw new IllegalArgumentException("codeVersion is required");
        }

        OrderBookSnapshotEvent lastSnapshot = lastSnapshot(batch);
        if (lastSnapshot == null) {
            return Optional.empty();
        }

        BigDecimal bidVolume = sumTopLevels(lastSnapshot.bids(), levelsToConsider);
        BigDecimal askVolume = sumTopLevels(lastSnapshot.asks(), levelsToConsider);
        BigDecimal totalVolume = bidVolume.add(askVolume, MATH_CONTEXT);

        BigDecimal obi = totalVolume.compareTo(BigDecimal.ZERO) == 0
                ? BigDecimal.ZERO.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN)
                : bidVolume.subtract(askVolume, MATH_CONTEXT)
                        .divide(totalVolume, RESULT_SCALE, RoundingMode.HALF_EVEN);

        int levelsUsed = Math.max(
                Math.min(levelsToConsider, lastSnapshot.bids().size()),
                Math.min(levelsToConsider, lastSnapshot.asks().size()));

        return Optional.of(new FeatureComputedEvent(
                UUIDv7.generate(),
                batch.windowEnd(),
                FEATURE_NAME,
                FEATURE_VERSION,
                obi,
                Map.of(
                        "bidVolume", bidVolume.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "askVolume", askVolume.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "levels", BigDecimal.valueOf(levelsUsed)
                ),
                batch.windowStart(),
                batch.windowEnd(),
                List.of(lastSnapshot.eventId()),
                codeVersion
        ));
    }

    private static OrderBookSnapshotEvent lastSnapshot(WindowedBatch batch) {
        OrderBookSnapshotEvent last = null;
        for (var event : batch.events()) {
            if (event instanceof OrderBookSnapshotEvent snapshot) {
                last = snapshot;
            }
        }
        return last;
    }

    private static BigDecimal sumTopLevels(List<PriceLevel> levels, int levelsToConsider) {
        BigDecimal sum = BigDecimal.ZERO;
        int count = Math.min(levelsToConsider, levels.size());
        for (int i = 0; i < count; i++) {
            sum = sum.add(levels.get(i).size(), MATH_CONTEXT);
        }
        return sum;
    }
}
