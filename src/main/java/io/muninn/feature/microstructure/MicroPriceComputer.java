package io.muninn.feature.microstructure;

import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pure-function Micro-Price (volume-weighted mid-price) computer.
 *
 * <p>Unlike the simple mid-price {@code (Bid + Ask) / 2}, the micro-price weighs each
 * side of the spread by the <strong>opposing</strong> volume, making it a better
 * indicator of where the price is likely to move next: a thin ask relative to the bid
 * pulls the fair value toward the ask, and vice versa.</p>
 *
 * <p>Formula: {@code (BidPrice * AskVol + AskPrice * BidVol) / (BidVol + AskVol)}.</p>
 *
 * <p>Uses {@link BigDecimal} arithmetic exclusively — floating-point is forbidden per
 * DETERMINISTIC_REPLAY.md §Anti-Patterns. This class has no state, no IO, no clock
 * reads; it is safe to call from both the live and replay paths, matching
 * {@code VwapComputer}'s discipline.</p>
 */
public final class MicroPriceComputer {

    /** Precision for intermediate calculations — matches {@code VwapComputer}. */
    private static final MathContext MATH_CONTEXT = new MathContext(18, RoundingMode.HALF_EVEN);

    /** Scale for the final micro-price value. */
    private static final int RESULT_SCALE = 8;

    public static final String FEATURE_NAME = "micro_price";
    public static final String FEATURE_VERSION = "v1";

    private MicroPriceComputer() {
        // Utility class — no instantiation
    }

    /**
     * Compute the micro-price from the most recent order-book snapshot in the window.
     *
     * <p>Like {@link OrderBookImbalanceComputer}, only the <strong>last</strong>
     * snapshot in the batch contributes — this is a point-in-time measure of book
     * state. Returns {@link Optional#empty()} if the window holds no
     * {@link OrderBookSnapshotEvent}, or if the snapshot's best bid or best ask is
     * missing entirely (an empty side of the book — nothing meaningful to compute).</p>
     *
     * @param batch       the windowed batch (may also contain trades — only
     *                    book-snapshot events are read)
     * @param codeVersion the git SHA or version of the feature engine
     * @return the computed feature event, or empty if there is no usable snapshot
     */
    public static Optional<FeatureComputedEvent> compute(WindowedBatch batch, String codeVersion) {
        if (codeVersion == null || codeVersion.isBlank()) {
            throw new IllegalArgumentException("codeVersion is required");
        }

        OrderBookSnapshotEvent lastSnapshot = lastSnapshot(batch);
        if (lastSnapshot == null || lastSnapshot.bids().isEmpty() || lastSnapshot.asks().isEmpty()) {
            return Optional.empty();
        }

        BigDecimal bestBidPrice = lastSnapshot.bids().get(0).price();
        BigDecimal bidVolume = lastSnapshot.bids().get(0).size();
        BigDecimal bestAskPrice = lastSnapshot.asks().get(0).price();
        BigDecimal askVolume = lastSnapshot.asks().get(0).size();

        BigDecimal totalVolume = bidVolume.add(askVolume, MATH_CONTEXT);

        BigDecimal microPrice;
        if (totalVolume.compareTo(BigDecimal.ZERO) == 0) {
            // Both top-of-book sizes are zero — fall back to the simple mid-price.
            microPrice = bestBidPrice.add(bestAskPrice, MATH_CONTEXT)
                    .divide(BigDecimal.valueOf(2), RESULT_SCALE, RoundingMode.HALF_EVEN);
        } else {
            // Cross-weighted: the side with LESS volume pulls price toward it less —
            // each price is weighted by the OPPOSING side's volume.
            BigDecimal weightedBid = bestBidPrice.multiply(askVolume, MATH_CONTEXT);
            BigDecimal weightedAsk = bestAskPrice.multiply(bidVolume, MATH_CONTEXT);
            microPrice = weightedBid.add(weightedAsk, MATH_CONTEXT)
                    .divide(totalVolume, RESULT_SCALE, RoundingMode.HALF_EVEN);
        }

        return Optional.of(new FeatureComputedEvent(
                UUIDv7.generate(),
                batch.windowEnd(),
                FEATURE_NAME,
                FEATURE_VERSION,
                microPrice,
                Map.of(
                        "bestBidPrice", bestBidPrice.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "bestAskPrice", bestAskPrice.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "bidVolume", bidVolume.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "askVolume", askVolume.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN)
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
}
