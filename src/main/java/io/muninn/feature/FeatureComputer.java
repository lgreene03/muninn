package io.muninn.feature;

import io.muninn.event.Event;

import java.util.Map;

public interface FeatureComputer {

    Map<String, Object> compute(FeatureDefinition definition, Iterable<Event> events);
}
