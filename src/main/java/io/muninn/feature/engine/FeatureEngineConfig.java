package io.muninn.feature.engine;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration for the feature engine.
 *
 * @param enabled            whether the feature engine is active
 * @param windowDuration     tumbling window size (default: 1 minute)
 * @param checkpointInterval how often to write checkpoints (in event time)
 * @param codeVersion        git SHA or version identifier for provenance tracking
 * @param lateEventPolicy    what to do with events that arrive after the watermark
 */
@ConfigurationProperties(prefix = "muninn.features.engine")
public record FeatureEngineConfig(
        boolean enabled,
        Duration windowDuration,
        Duration checkpointInterval,
        String codeVersion,
        String lateEventPolicy
) {

    public FeatureEngineConfig {
        if (windowDuration == null) windowDuration = Duration.ofMinutes(1);
        if (checkpointInterval == null) checkpointInterval = Duration.ofMinutes(5);
        if (codeVersion == null || codeVersion.isBlank()) codeVersion = "dev";
        if (lateEventPolicy == null || lateEventPolicy.isBlank()) lateEventPolicy = "drop";
    }
}
