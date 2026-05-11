package io.muninn.shared.event;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The output of the feature engine. It is itself an event so that downstream consumers
 * can subscribe to derived signals using the same machinery as raw events.
 *
 * <p>Every computed event carries full provenance: the feature name, the code version
 * that produced it, and the list of input event IDs that contributed to the output.
 * This allows any consumer to request the replay engine to re-derive the value and compare.</p>
 *
 * @param eventId       UUIDv7 identifier for this computed event
 * @param eventTime     the event time of the triggering input (not wall-clock)
 * @param featureName   the registered feature name (e.g., {@code "vwap.1m"})
 * @param featureVersion semantic version or git SHA of the feature definition
 * @param value         the computed scalar value (nullable if the feature produces a map)
 * @param values        the computed map value (nullable if the feature produces a scalar)
 * @param windowStart   start of the computation window
 * @param windowEnd     end of the computation window
 * @param inputEventIds provenance — the event IDs consumed to produce this output
 * @param codeVersion   git SHA of the feature engine that produced this
 */
public record FeatureComputedEvent(
        @JsonProperty(required = true) UUID eventId,
        @JsonProperty(required = true) Instant eventTime,
        @JsonProperty(required = true) String featureName,
        @JsonProperty(required = true) String featureVersion,
        BigDecimal value,
        Map<String, BigDecimal> values,
        @JsonProperty(required = true) Instant windowStart,
        @JsonProperty(required = true) Instant windowEnd,
        @JsonProperty(required = true) List<UUID> inputEventIds,
        @JsonProperty(required = true) String codeVersion
) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public FeatureComputedEvent {
        if (eventId == null) throw new IllegalArgumentException("FeatureComputedEvent eventId is required");
        if (eventTime == null) throw new IllegalArgumentException("FeatureComputedEvent eventTime is required");
        if (featureName == null || featureName.isBlank()) throw new IllegalArgumentException("FeatureComputedEvent featureName is required");
        if (featureVersion == null || featureVersion.isBlank()) throw new IllegalArgumentException("FeatureComputedEvent featureVersion is required");
        if (windowStart == null) throw new IllegalArgumentException("FeatureComputedEvent windowStart is required");
        if (windowEnd == null) throw new IllegalArgumentException("FeatureComputedEvent windowEnd is required");
        if (inputEventIds == null) throw new IllegalArgumentException("FeatureComputedEvent inputEventIds is required");
        if (codeVersion == null || codeVersion.isBlank()) throw new IllegalArgumentException("FeatureComputedEvent codeVersion is required");
        if (value == null && values == null) throw new IllegalArgumentException("FeatureComputedEvent must have either value or values");
        // Defensive copies
        inputEventIds = List.copyOf(inputEventIds);
        if (values != null) {
            values = Map.copyOf(values);
        }
    }

    /**
     * The Kafka topic for this feature's outputs, derived from name and version.
     */
    public String topicName() {
        return "features." + featureName + "." + featureVersion;
    }
}
