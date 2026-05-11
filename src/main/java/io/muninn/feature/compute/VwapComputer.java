package io.muninn.feature.compute;

import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;

/**
 * Pure-function VWAP (Volume-Weighted Average Price) computer.
 *
 * <p>Computes: {@code VWAP = Σ(price × size) / Σ(size)} over a windowed batch of trades.
 * Uses {@link BigDecimal} arithmetic exclusively — floating-point is forbidden
 * per DETERMINISTIC_REPLAY.md §Anti-Patterns.</p>
 *
 * <p>This class has no state, no IO, no clock reads. It is safe to call from both
 * the live and replay paths.</p>
 */
public final class VwapComputer {

    /** Precision for intermediate and final VWAP calculations. */
    private static final MathContext MATH_CONTEXT = new MathContext(18, RoundingMode.HALF_EVEN);

    /** Scale for the final VWAP value (8 decimal places — sub-satoshi precision for BTC). */
    private static final int RESULT_SCALE = 8;

    public static final String FEATURE_NAME = "vwap.1m";
    public static final String FEATURE_VERSION = "v1";

    private VwapComputer() {
        // Utility class — no instantiation
    }

    /**
     * Compute the VWAP for a windowed batch of trades.
     *
     * <p>Returns a {@link FeatureComputedEvent} with full provenance. If the window
     * contains only zero-size trades (Σsize = 0), the VWAP is the simple average
     * of prices — this avoids division by zero while preserving a meaningful value.</p>
     *
     * @param batch       the windowed batch of trades
     * @param codeVersion the git SHA or version of the feature engine
     * @return the computed feature event
     * @throws IllegalArgumentException if the batch is empty
     */
    public static FeatureComputedEvent compute(WindowedBatch batch, String codeVersion) {
        if (batch.isEmpty()) {
            throw new IllegalArgumentException("Cannot compute VWAP for an empty window");
        }
        if (codeVersion == null || codeVersion.isBlank()) {
            throw new IllegalArgumentException("codeVersion is required");
        }

        BigDecimal sumPriceTimesSize = BigDecimal.ZERO;
        BigDecimal sumSize = BigDecimal.ZERO;
        var inputEventIds = new java.util.ArrayList<java.util.UUID>();

        for (TradeEvent trade : batch.trades()) {
            BigDecimal priceTimesSize = trade.price().multiply(trade.size(), MATH_CONTEXT);
            sumPriceTimesSize = sumPriceTimesSize.add(priceTimesSize, MATH_CONTEXT);
            sumSize = sumSize.add(trade.size(), MATH_CONTEXT);
            inputEventIds.add(trade.eventId());
        }

        BigDecimal vwap;
        if (sumSize.compareTo(BigDecimal.ZERO) == 0) {
            // All trades have zero size — fall back to simple average of prices
            BigDecimal sumPrices = BigDecimal.ZERO;
            for (TradeEvent trade : batch.trades()) {
                sumPrices = sumPrices.add(trade.price(), MATH_CONTEXT);
            }
            vwap = sumPrices.divide(BigDecimal.valueOf(batch.size()), RESULT_SCALE, RoundingMode.HALF_EVEN);
        } else {
            vwap = sumPriceTimesSize.divide(sumSize, RESULT_SCALE, RoundingMode.HALF_EVEN);
        }

        return new FeatureComputedEvent(
                UUIDv7.generate(),
                batch.windowEnd(),            // eventTime = window close time
                FEATURE_NAME,
                FEATURE_VERSION,
                vwap,                          // scalar value
                Map.of(                        // additional values for richer consumers
                        "sumPriceTimesSize", sumPriceTimesSize.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "sumSize", sumSize.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "tradeCount", BigDecimal.valueOf(batch.size())
                ),
                batch.windowStart(),
                batch.windowEnd(),
                inputEventIds,
                codeVersion
        );
    }
}
