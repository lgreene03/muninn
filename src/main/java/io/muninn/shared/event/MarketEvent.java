package io.muninn.shared.event;

import io.muninn.shared.instrument.Instrument;

import java.time.Instant;
import java.util.UUID;

/**
 * Supertype of all market-data events.
 *
 * <p>Sealed to the canonical event types defined in DOMAIN_MODEL.md. Every implementation
 * is an immutable {@code record}. Events carry both event-time (when it happened in the world)
 * and ingest-time (when Muninn observed it). Feature computation uses event-time only;
 * processing-time is for SLA tracking and operational metrics.</p>
 *
 * <p>All event IDs are UUIDv7 (time-ordered). Sequence numbers are monotonic per
 * {@code (source, instrument)} partition.</p>
 *
 * @see TradeEvent
 * @see CandleEvent
 * @see OrderBookSnapshotEvent
 */
public sealed interface MarketEvent
        permits TradeEvent, CandleEvent, OrderBookSnapshotEvent, OrderDeltaEvent {

    /** UUIDv7 identifier, time-ordered and globally unique. */
    UUID eventId();

    /** When the fact occurred in the world, as reported by the source. */
    Instant eventTime();

    /** When Muninn's ingestion layer observed this event. */
    Instant ingestTime();

    /** Adapter identifier (e.g., {@code "binance.spot.v1"}). */
    String source();

    /** The instrument this event pertains to. */
    Instrument instrument();

    /** Monotonic sequence number per {@code (source, instrument)}. */
    long sequenceNumber();

    /** Schema version integer, incremented on any schema change. */
    int schemaVersion();

    /** The Kafka topic this event should be routed to. */
    String topicName();
}
