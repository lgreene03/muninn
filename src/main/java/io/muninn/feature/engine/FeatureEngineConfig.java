package io.muninn.feature.engine;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Configuration for the feature engine.
 *
 * @param enabled              whether the feature engine is active
 * @param windowDuration       tumbling window size (default: 1 minute)
 * @param checkpointInterval   how often to write checkpoints (in event time)
 * @param codeVersion          git SHA or version identifier for provenance tracking
 * @param lateEventPolicy      what to do with events that arrive after the watermark
 * @param obiLevels            depth (levels per side) used by {@code OrderBookImbalanceComputer};
 *                             default 10, matching the top-of-book depth the live path reads
 * @param vpinBucketVolumeSize the trade volume that fills one VPIN bucket ({@code V})
 * @param vpinNumberOfBuckets  how many trailing VPIN buckets the moving average covers ({@code n})
 * @param instrumentExchange   the venue the engine computes, matching {@code instrument.exchange.id}
 * @param instrumentSymbol     the symbol the engine computes, matching {@code instrument.symbol}.
 *                             Also the Kafka key outputs are published under, so the label on
 *                             a feature can never disagree with what it was computed from.
 *                             See {@link InstrumentFence} for why one engine computes one
 *                             instrument.
 */
@ConfigurationProperties(prefix = "muninn.features.engine")
public record FeatureEngineConfig(
        boolean enabled,
        Duration windowDuration,
        Duration checkpointInterval,
        String codeVersion,
        String lateEventPolicy,
        Integer obiLevels,
        BigDecimal vpinBucketVolumeSize,
        Integer vpinNumberOfBuckets,
        String instrumentExchange,
        String instrumentSymbol
) {

    public FeatureEngineConfig {
        if (windowDuration == null) windowDuration = Duration.ofMinutes(1);
        if (checkpointInterval == null) checkpointInterval = Duration.ofMinutes(5);
        if (codeVersion == null || codeVersion.isBlank()) codeVersion = "dev";
        if (lateEventPolicy == null || lateEventPolicy.isBlank()) lateEventPolicy = "drop";
        if (obiLevels == null || obiLevels <= 0) obiLevels = 10;
        if (vpinBucketVolumeSize == null || vpinBucketVolumeSize.signum() <= 0) {
            vpinBucketVolumeSize = new BigDecimal("1");
        }
        if (vpinNumberOfBuckets == null || vpinNumberOfBuckets <= 0) vpinNumberOfBuckets = 50;
        // Defaults are what the engine has always computed, so the fence changes nothing
        // until a second venue or symbol is actually ingested.
        if (instrumentExchange == null || instrumentExchange.isBlank()) instrumentExchange = "binance";
        if (instrumentSymbol == null || instrumentSymbol.isBlank()) instrumentSymbol = "BTC-USDT";
    }

    /** The admission policy these settings describe. */
    public InstrumentFence fence() {
        return new InstrumentFence(instrumentExchange, instrumentSymbol);
    }
}
