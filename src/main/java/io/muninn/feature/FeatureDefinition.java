package io.muninn.feature;

import java.time.Duration;

public record FeatureDefinition(
        String name,
        String description,
        String sourceTopic,
        String aggregation,
        Duration window,
        String expression
) {

    public enum Aggregation {
        COUNT, SUM, AVG, MIN, MAX, LAST, FIRST, STDDEV
    }
}
