package io.muninn.feature.microstructure;

import io.muninn.feature.FeatureComputer;
import io.muninn.feature.FeatureDefinition;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;

import java.util.Map;

/**
 * Computes the Order Book Imbalance (OBI).
 *
 * <p>OBI is a classic microstructure feature used to predict short-term price movements.
 * Formula: {@code (Bid Volume - Ask Volume) / (Bid Volume + Ask Volume)}.</p>
 *
 * <p>A value of +1.0 indicates extreme buy pressure, -1.0 indicates extreme sell pressure.</p>
 */
public class OrderBookImbalanceComputer implements FeatureComputer {

    private final int levelsToConsider;

    public OrderBookImbalanceComputer(int levelsToConsider) {
        if (levelsToConsider <= 0) {
            throw new IllegalArgumentException("Levels must be positive");
        }
        this.levelsToConsider = levelsToConsider;
    }

    @Override
    public Map<String, Object> compute(FeatureDefinition definition, Iterable<MarketEvent> events) {
        OrderBookSnapshotEvent lastSnapshot = null;
        for (MarketEvent event : events) {
            if (event instanceof OrderBookSnapshotEvent snapshot) {
                lastSnapshot = snapshot;
            }
        }

        if (lastSnapshot == null) {
            return Map.of("obi", 0.0);
        }

        double bidVolume = 0.0;
        int bidsCount = Math.min(levelsToConsider, lastSnapshot.bids().size());
        for (int i = 0; i < bidsCount; i++) {
            bidVolume += lastSnapshot.bids().get(i).size().doubleValue();
        }

        double askVolume = 0.0;
        int asksCount = Math.min(levelsToConsider, lastSnapshot.asks().size());
        for (int i = 0; i < asksCount; i++) {
            askVolume += lastSnapshot.asks().get(i).size().doubleValue();
        }

        double totalVolume = bidVolume + askVolume;
        double obi = totalVolume == 0.0 ? 0.0 : (bidVolume - askVolume) / totalVolume;

        return Map.of("obi", obi, "levels", Math.max(bidsCount, asksCount));
    }
}
