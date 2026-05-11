# DOMAIN_MODEL.md

This document defines Muninn's core domain vocabulary. Every term here has a precise meaning. When in doubt, refer back to this document; do not re-define terms locally.

## Events

All events are immutable records. They carry an `eventId` (UUIDv7, sortable by creation time), an `eventTime` (when the fact occurred in the world), and a `source` identifying their origin.

### MarketEvent (abstract)

The supertype of all market-data events. Carries:

- `eventId: UUID`
- `eventTime: Instant` — exchange-reported timestamp
- `ingestTime: Instant` — when Muninn observed it
- `source: String` — adapter id (e.g., `coinbase.spot.v1`)
- `instrument: Instrument`
- `sequenceNumber: long` — monotonic per (source, instrument)

### TradeEvent

A single executed trade reported by an exchange.

- price: `BigDecimal`
- size: `BigDecimal`
- side: `Side` (BUY | SELL | UNKNOWN)
- exchangeTradeId: `String`

### CandleEvent

A time-bucketed OHLCV summary. Produced either by the exchange (preferred) or by the feature engine. When produced internally, it is itself reproducible from `TradeEvent`s.

- open, high, low, close: `BigDecimal`
- volume: `BigDecimal`
- windowStart, windowEnd: `Instant`
- windowDuration: `Duration`

### OrderBookSnapshotEvent

A point-in-time order-book state, capped to a configured depth.

- bids: `List<PriceLevel>`
- asks: `List<PriceLevel>`
- depth: `int`
- sequenceNumber: `long`

`PriceLevel = (price, size)`.

### FeatureComputedEvent

The output of the feature engine. It is itself an event so that downstream consumers can subscribe to derived signals using the same machinery as raw events.

- featureName: `String`
- featureVersion: `String`
- value: `BigDecimal | Map<String, BigDecimal>`
- windowStart, windowEnd: `Instant`
- inputEventIds: `List<UUID>` — provenance
- codeVersion: `String` — git SHA of the feature engine that produced this

## Aggregates and Reference Data

### Instrument

A trade-able instrument on an exchange.

- symbol: `String` (e.g., `BTC-USD`)
- baseAsset: `String`
- quoteAsset: `String`
- exchange: `Exchange`

### Exchange

A data source.

- id: `String` (e.g., `coinbase`)
- displayName: `String`
- timezone: `ZoneId`

## Time

### EventTime

The time the fact occurred in the world, as reported by the source. This is the primary time used for windowing, ordering, and feature computation. It can arrive out-of-order.

### ProcessingTime

The time the system observed or processed the event. Used for SLAs, lag metrics, and operational dashboards. **Never used inside feature computation.**

### Watermark

A monotonic estimate of "we have seen all events with `eventTime` ≤ W." A window closes when its end is below the current watermark. Late events arriving after their window's watermark are routed to a configured policy (drop, side-output, or revise).

## Replay

### ReplayJob

A scheduled or ad-hoc replay of a portion of the event log through the feature engine.

- jobId: `UUID`
- from, to: `Instant` — event-time range
- topics: `List<String>`
- featureVersion: `String`
- outputSink: `String` (e.g., `parquet://muninn-warehouse/replay/{jobId}`)
- status: `PENDING | RUNNING | COMPLETED | FAILED`
- checkpoints: `List<Checkpoint>`

### FeatureWindow

The bounded time range over which a feature is computed.

- featureName: `String`
- windowStart, windowEnd: `Instant`
- windowType: `TUMBLING | SLIDING | SESSION`
- size, slide: `Duration`

### Checkpoint

A durable snapshot of feature-engine state, indexed by watermark. Replay can resume from a checkpoint rather than re-reading from the beginning.

- checkpointId: `UUID`
- watermark: `Instant`
- engineVersion: `String`
- stateSnapshotUri: `String`

## Identity and Versioning

- All `UUID` fields are UUIDv7 (time-ordered).
- `featureVersion` and `codeVersion` are git SHAs or semantic version strings; they are recorded on every `FeatureComputedEvent` so that consumers can detect logic changes.
- Schema evolution is tracked separately in [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md).
