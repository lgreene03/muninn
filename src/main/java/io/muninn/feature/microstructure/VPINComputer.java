package io.muninn.feature.microstructure;

import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Pure-function Volume-Synchronized Probability of Informed Trading (VPIN) computer.
 *
 * <p>VPIN measures order-flow toxicity. Unlike time-based metrics, VPIN operates on
 * <strong>volume buckets</strong>: once a bucket accumulates {@code bucketVolumeSize}
 * of trade volume, the bucket's buy/sell imbalance is recorded, and VPIN is the moving
 * average of the last {@code numberOfBuckets} imbalances, normalized by total bucketed
 * volume.</p>
 *
 * <p>Volume buckets do not align to the engine's time windows — a bucket may span
 * several windows, or several buckets may fill within a single window. This class is
 * therefore called once per closed window like every other computer, but threads an
 * explicit, immutable {@link BucketState} across calls rather than holding any mutable
 * field: {@link #compute} is a pure function of {@code (state, batch, ...)} and returns
 * the next state alongside its result. The caller (the feature engine) owns the state,
 * keyed per instrument, exactly as it owns open-window state in {@code WindowManager}.
 * This is the fix for the defect described in {@code FeatureComputer}'s own contract —
 * a computer must not carry mutable instance state, and must not silently share state
 * across instruments.</p>
 *
 * <p>Fixes applied relative to the original implementation (see
 * {@code DETERMINISTIC_REPLAY.md} §Anti-Patterns and the P2 roadmap):</p>
 * <ul>
 *   <li><strong>Bucket carryover.</strong> A trade that overshoots the current bucket's
 *       remaining capacity is <em>split</em> at the boundary: enough of its volume fills
 *       the current bucket exactly, the bucket is closed, and the remainder starts the
 *       next bucket (looping again if the trade is larger than a full bucket). No volume
 *       is ever discarded, so bucket volume never exceeds {@code bucketVolumeSize} and the
 *       {@code numberOfBuckets * bucketVolumeSize} denominator is exact.</li>
 *   <li><strong>No mutable instance state, keyed by instrument.</strong> See above.</li>
 *   <li><strong>{@link BigDecimal} arithmetic throughout</strong> — no {@code double}.</li>
 *   <li><strong>No {@code NaN}.</strong> While fewer than {@code numberOfBuckets} buckets
 *       have filled, {@link #compute} returns an empty {@link Result#event()} rather than
 *       a sentinel value; {@code bucketsFilled} (observable via {@link BucketState}, and
 *       via the emitted event's {@code values} map once ready) carries readiness instead.</li>
 *   <li><strong>Unknown-side volume is observable.</strong> Binance aggTrades always carry
 *       {@code isBuyerMaker}, so {@link Side#UNKNOWN} should be rare-to-never in practice.
 *       The 50/50 split fallback is kept (it is the least-bad option when side truly is
 *       unknown), but the volume split this way is summed and returned via
 *       {@link Result#unknownSideVolumeProcessed()} so the caller can raise a metric on it
 *       rather than let it pass silently.</li>
 * </ul>
 */
public final class VPINComputer {

    /** Precision for intermediate calculations — matches {@code VwapComputer}. */
    private static final MathContext MATH_CONTEXT = new MathContext(18, RoundingMode.HALF_EVEN);

    /** Scale for the final VPIN value. */
    private static final int RESULT_SCALE = 8;

    public static final String FEATURE_NAME = "vpin";
    public static final String FEATURE_VERSION = "v1";

    private VPINComputer() {
        // Utility class — no instantiation
    }

    /**
     * Immutable volume-bucket accumulation state, carried explicitly across window
     * calls by the caller (never held as mutable instance state on this class).
     *
     * @param currentBucketBuyVolume  buy volume accumulated in the in-progress bucket
     * @param currentBucketSellVolume sell volume accumulated in the in-progress bucket
     * @param completedBucketImbalances the most recent bucket imbalances, oldest first,
     *                                   bounded to the configured {@code numberOfBuckets}
     */
    public record BucketState(
            BigDecimal currentBucketBuyVolume,
            BigDecimal currentBucketSellVolume,
            List<BigDecimal> completedBucketImbalances
    ) {
        public static final BucketState EMPTY =
                new BucketState(BigDecimal.ZERO, BigDecimal.ZERO, List.of());

        public BucketState {
            if (currentBucketBuyVolume == null) currentBucketBuyVolume = BigDecimal.ZERO;
            if (currentBucketSellVolume == null) currentBucketSellVolume = BigDecimal.ZERO;
            completedBucketImbalances = completedBucketImbalances == null
                    ? List.of() : List.copyOf(completedBucketImbalances);
        }

        /** @return how many of the configured buckets have filled so far */
        public int bucketsFilled() {
            return completedBucketImbalances.size();
        }
    }

    /**
     * @param event                     the computed VPIN event, or empty if fewer than
     *                                  {@code numberOfBuckets} have filled yet, or this
     *                                  window contained no trades
     * @param nextState                 the bucket state to pass into the next call for
     *                                  this instrument
     * @param unknownSideVolumeProcessed total trade volume this call allocated via the
     *                                  50/50 unknown-side fallback — the caller should
     *                                  raise a metric on any non-zero value
     */
    public record Result(
            Optional<FeatureComputedEvent> event,
            BucketState nextState,
            BigDecimal unknownSideVolumeProcessed
    ) {
    }

    /**
     * Fold this window's trades into the running volume-bucket state and, if enough
     * buckets have filled, compute the current VPIN reading.
     *
     * @param state            the bucket state carried from the previous call for this
     *                         instrument ({@link BucketState#EMPTY} on a cold start)
     * @param batch            the windowed batch (may also contain non-trade events —
     *                         only trades are read)
     * @param bucketVolumeSize the trade volume that fills one bucket ({@code V})
     * @param numberOfBuckets  how many trailing buckets the moving average covers ({@code n})
     * @param codeVersion      the git SHA or version of the feature engine
     * @return the result: an optional feature event plus the state to carry forward
     */
    public static Result compute(
            BucketState state,
            WindowedBatch batch,
            BigDecimal bucketVolumeSize,
            int numberOfBuckets,
            String codeVersion) {
        if (bucketVolumeSize == null || bucketVolumeSize.signum() <= 0) {
            throw new IllegalArgumentException("bucketVolumeSize must be positive");
        }
        if (numberOfBuckets <= 0) {
            throw new IllegalArgumentException("numberOfBuckets must be positive");
        }
        if (codeVersion == null || codeVersion.isBlank()) {
            throw new IllegalArgumentException("codeVersion is required");
        }
        BucketState current = state == null ? BucketState.EMPTY : state;

        BigDecimal buyVolume = current.currentBucketBuyVolume();
        BigDecimal sellVolume = current.currentBucketSellVolume();
        List<BigDecimal> imbalances = new ArrayList<>(current.completedBucketImbalances());
        BigDecimal unknownSideVolume = BigDecimal.ZERO;
        List<UUID> inputEventIds = new ArrayList<>();

        for (TradeEvent trade : batch.trades()) {
            inputEventIds.add(trade.eventId());

            if (trade.side() == Side.UNKNOWN) {
                unknownSideVolume = unknownSideVolume.add(trade.size(), MATH_CONTEXT);
            }

            BigDecimal remaining = trade.size();
            while (remaining.signum() > 0) {
                BigDecimal capacity = bucketVolumeSize.subtract(
                        buyVolume.add(sellVolume, MATH_CONTEXT), MATH_CONTEXT);
                if (capacity.signum() <= 0) {
                    // Defensive only: a fresh bucket always has full capacity, since we
                    // flush exactly when a bucket reaches bucketVolumeSize below.
                    capacity = bucketVolumeSize;
                }
                BigDecimal portion = remaining.compareTo(capacity) <= 0 ? remaining : capacity;

                switch (trade.side()) {
                    case BUY -> buyVolume = buyVolume.add(portion, MATH_CONTEXT);
                    case SELL -> sellVolume = sellVolume.add(portion, MATH_CONTEXT);
                    case UNKNOWN -> {
                        BigDecimal half = portion.divide(BigDecimal.TWO, MATH_CONTEXT);
                        buyVolume = buyVolume.add(half, MATH_CONTEXT);
                        sellVolume = sellVolume.add(half, MATH_CONTEXT);
                    }
                }
                remaining = remaining.subtract(portion, MATH_CONTEXT);

                if (buyVolume.add(sellVolume, MATH_CONTEXT).compareTo(bucketVolumeSize) >= 0) {
                    // Bucket is full — record its imbalance and split the trade's
                    // remainder (if any) into a fresh bucket on the next loop iteration.
                    BigDecimal imbalance = buyVolume.subtract(sellVolume, MATH_CONTEXT).abs();
                    imbalances.add(imbalance);
                    while (imbalances.size() > numberOfBuckets) {
                        imbalances.remove(0);
                    }
                    buyVolume = BigDecimal.ZERO;
                    sellVolume = BigDecimal.ZERO;
                }
            }
        }

        BucketState nextState = new BucketState(buyVolume, sellVolume, imbalances);

        if (inputEventIds.isEmpty() || imbalances.size() < numberOfBuckets) {
            // Either no trades this window, or not enough bucket history yet —
            // bucketsFilled (via nextState) carries readiness; no NaN, no event.
            return new Result(Optional.empty(), nextState, unknownSideVolume);
        }

        BigDecimal sumImbalance = BigDecimal.ZERO;
        for (BigDecimal imbalance : imbalances) {
            sumImbalance = sumImbalance.add(imbalance, MATH_CONTEXT);
        }
        BigDecimal denominator = BigDecimal.valueOf(numberOfBuckets).multiply(bucketVolumeSize, MATH_CONTEXT);
        BigDecimal vpin = sumImbalance.divide(denominator, RESULT_SCALE, RoundingMode.HALF_EVEN);

        FeatureComputedEvent event = new FeatureComputedEvent(
                UUIDv7.generate(),
                batch.windowEnd(),
                FEATURE_NAME,
                FEATURE_VERSION,
                vpin,
                Map.of(
                        "bucketsFilled", BigDecimal.valueOf(imbalances.size()),
                        "bucketVolumeSize", bucketVolumeSize.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN),
                        "unknownSideVolume", unknownSideVolume.setScale(RESULT_SCALE, RoundingMode.HALF_EVEN)
                ),
                batch.windowStart(),
                batch.windowEnd(),
                inputEventIds,
                codeVersion
        );

        return new Result(Optional.of(event), nextState, unknownSideVolume);
    }
}
