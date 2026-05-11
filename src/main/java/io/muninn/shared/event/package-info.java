/**
 * Canonical event types for Muninn.
 *
 * <p>All market-data events implement {@link io.muninn.shared.event.MarketEvent}, a sealed interface
 * that ensures exhaustive pattern matching. Event types are immutable records with construction-time
 * validation. Prices and sizes use {@link java.math.BigDecimal} to avoid floating-point drift
 * during deterministic replay.</p>
 *
 * <p>Serialization is JSON via Jackson in the MVP (Phases 1–3). Migration to Avro with
 * schema registry is planned for Phase 4+ per EVENT_SCHEMA_STRATEGY.md.</p>
 *
 * @see io.muninn.shared.event.MarketEvent
 * @see io.muninn.shared.event.TradeEvent
 * @see io.muninn.shared.event.CandleEvent
 * @see io.muninn.shared.event.OrderBookSnapshotEvent
 * @see io.muninn.shared.event.FeatureComputedEvent
 */
package io.muninn.shared.event;
