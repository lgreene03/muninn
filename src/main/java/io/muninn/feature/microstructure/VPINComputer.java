package io.muninn.feature.microstructure;

import io.muninn.feature.FeatureComputer;
import io.muninn.feature.FeatureDefinition;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;

import java.util.LinkedList;
import java.util.Map;

/**
 * Computes Volume-Synchronized Probability of Informed Trading (VPIN).
 *
 * <p>VPIN measures order flow toxicity. Unlike time-based metrics, VPIN operates on
 * "volume buckets." Once a bucket fills with $V$ volume, the imbalance is calculated.
 * VPIN is the moving average of these absolute imbalances over $N$ buckets.</p>
 */
public class VPINComputer implements FeatureComputer {

    private final double bucketVolumeSize;
    private final int numberOfBuckets;

    // Stateful fields (in Phase 10 we move these to a state store, but for Phase 9 demo they are here)
    private double currentBucketBuyVol = 0.0;
    private double currentBucketSellVol = 0.0;
    private final LinkedList<Double> bucketImbalances = new LinkedList<>();

    public VPINComputer(double bucketVolumeSize, int numberOfBuckets) {
        if (bucketVolumeSize <= 0 || numberOfBuckets <= 0) {
            throw new IllegalArgumentException("Bucket volume and count must be positive");
        }
        this.bucketVolumeSize = bucketVolumeSize;
        this.numberOfBuckets = numberOfBuckets;
    }

    @Override
    public Map<String, Object> compute(FeatureDefinition definition, Iterable<MarketEvent> events) {
        double vpin = Double.NaN;
        
        for (MarketEvent event : events) {
            if (event instanceof TradeEvent trade) {
                double size = trade.size().doubleValue();
                if (trade.side() == Side.BUY) {
                    currentBucketBuyVol += size;
                } else if (trade.side() == Side.SELL) {
                    currentBucketSellVol += size;
                } else {
                    // Unknown side, split evenly
                    currentBucketBuyVol += (size / 2.0);
                    currentBucketSellVol += (size / 2.0);
                }

                double totalBucketVol = currentBucketBuyVol + currentBucketSellVol;
                if (totalBucketVol >= bucketVolumeSize) {
                    // Bucket is full, record imbalance
                    double imbalance = Math.abs(currentBucketBuyVol - currentBucketSellVol);
                    bucketImbalances.addLast(imbalance);
                    
                    if (bucketImbalances.size() > numberOfBuckets) {
                        bucketImbalances.removeFirst();
                    }
                    
                    // Reset bucket (carryover logic omitted for simplicity in Phase 9 demo)
                    currentBucketBuyVol = 0.0;
                    currentBucketSellVol = 0.0;
                }
            }
        }

        if (bucketImbalances.size() == numberOfBuckets) {
            double sumImbalance = bucketImbalances.stream().mapToDouble(Double::doubleValue).sum();
            vpin = sumImbalance / (numberOfBuckets * bucketVolumeSize);
        }

        return Map.of(
            "vpin", vpin,
            "bucketsFilled", bucketImbalances.size()
        );
    }
}
