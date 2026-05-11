package io.muninn.feature;

import io.muninn.shared.event.MarketEvent;

import java.util.Map;

/**
 * Contract for feature computation.
 *
 * <p>A feature computer is a <strong>pure function</strong>: given a definition and a sequence
 * of events, it produces a deterministic output. No wall-clock reads, no external IO,
 * no mutable static state.</p>
 *
 * <p>This interface will be expanded in Phase 3 with state management, windowing,
 * and checkpoint support.</p>
 */
public interface FeatureComputer {

    Map<String, Object> compute(FeatureDefinition definition, Iterable<MarketEvent> events);
}
