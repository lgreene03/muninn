/**
 * Feature computation implementations — pure functions.
 *
 * <p>Each feature computer is a stateless function that takes a windowed batch
 * of events and produces a {@link io.muninn.shared.event.FeatureComputedEvent}.
 * No wall-clock reads, no IO, no mutable state.</p>
 */
package io.muninn.feature.compute;
