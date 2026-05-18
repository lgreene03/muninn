package io.muninn.feature.microstructure;

import io.muninn.feature.FeatureComputer;
import io.muninn.feature.FeatureDefinition;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;

import java.util.Map;

/**
 * Computes the Micro-Price (Volume-Weighted Mid-Price).
 *
 * <p>Unlike the standard mid-price (Bid + Ask) / 2, the Micro-Price weighs the
 * spread by the opposing volume, making it an excellent indicator of true fair value
 * in a microstructural setting.</p>
 *
 * <p>Formula: {@code (BidPrice * AskVol + AskPrice * BidVol) / (BidVol + AskVol)}</p>
 */
public class MicroPriceComputer implements FeatureComputer {

    @Override
    public Map<String, Object> compute(FeatureDefinition definition, Iterable<MarketEvent> events) {
        OrderBookSnapshotEvent lastSnapshot = null;
        for (MarketEvent event : events) {
            if (event instanceof OrderBookSnapshotEvent snapshot) {
                lastSnapshot = snapshot;
            }
        }

        if (lastSnapshot == null || lastSnapshot.bids().isEmpty() || lastSnapshot.asks().isEmpty()) {
            return Map.of("microPrice", Double.NaN);
        }

        double bestBid = lastSnapshot.bids().get(0).price().doubleValue();
        double bidVol = lastSnapshot.bids().get(0).size().doubleValue();

        double bestAsk = lastSnapshot.asks().get(0).price().doubleValue();
        double askVol = lastSnapshot.asks().get(0).size().doubleValue();

        double totalVol = bidVol + askVol;
        if (totalVol == 0.0) {
            return Map.of("microPrice", (bestBid + bestAsk) / 2.0);
        }

        double microPrice = (bestBid * askVol + bestAsk * bidVol) / totalVol;

        return Map.of("microPrice", microPrice);
    }
}
