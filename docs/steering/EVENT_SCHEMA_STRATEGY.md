# EVENT_SCHEMA_STRATEGY.md

Schemas are the contract between event producers, the event log, and every downstream consumer (live and replay). They are the thing that breaks silently if not managed deliberately.

## Format Comparison

| Property | JSON | Avro | Protobuf |
|---|---|---|---|
| Human-readable | Yes | No (binary) | No (binary) |
| Self-describing | Partial | With registry | With descriptors |
| Schema evolution | Ad-hoc | Strong, schema registry | Strong, FieldOptions |
| Size on wire | Largest | Compact | Compact |
| Tooling (Java) | Excellent | Excellent | Excellent |
| Tooling (Parquet) | Native | First-class | Via Avro bridge |
| Learning curve | None | Moderate | Moderate |
| Breaking changes caught at compile time | No | No (runtime) | Yes |
| Field naming flexibility | High | Moderate | Low |
| Forward/backward compat | Manual | Built-in via reader/writer schemas | Built-in via field numbers |
| Local dev friction | Lowest | Schema registry required | `protoc` step required |

## MVP Recommendation: **JSON, with a registry-backed migration path**

For Phase 1–3 (local ingestion, canonical events, feature engine), use **JSON over Kafka** with Jackson on the Java side.

Rationale:

- **Local-first wins.** No schema registry to run. Producers and consumers debug with `kafka-console-consumer` and human eyes.
- **The bottleneck is correctness, not size.** At MVP volumes (≤ 10k events/sec for a single instrument), wire size is irrelevant.
- **Iteration speed.** Adding a field is a Jackson change, not a registry migration.
- **DuckDB and Parquet handle JSON well.** Parquet write goes via a typed Java record; the wire format is decoupled.

The risk is that JSON has no enforcement. We mitigate this with:

1. **A canonical Java record** for each event type, with Jackson annotations and `@JsonProperty(required = true)` where applicable.
2. **A schema validation step** at the ingestion boundary that rejects malformed events with a structured error and a metric.
3. **A `schemaVersion` field** on every event, defaulting to `1`. Consumers branch on it explicitly when evolution requires.

## Migration Path: Avro at Phase 4+

When the system has more than one producer, or when an external team consumes the event log, migrate to **Avro with a Confluent-compatible schema registry** (Redpanda has one built in).

Triggers for the migration:

- A second producer is added (multi-exchange).
- A backward-incompatible change is required and JSON's lack of enforcement is biting.
- The event log is exposed to external consumers.

The migration is mechanical: the Java records remain; serializers swap from `JsonSerializer` to `KafkaAvroSerializer`. Consumers updated topic-by-topic, not big-bang.

## Why Not Protobuf in MVP

Protobuf is excellent for RPC and tightly-coupled service contracts. For an event log meant to be human-readable during early development, the `protoc` build step and binary wire format add friction without proportional benefit. Reconsider at Phase 8 if the production-reference profile demands it.

## Schema Evolution Rules

Regardless of format:

1. **Adding a nullable field** is always allowed. Default to `null` for old events.
2. **Adding a required field** is forbidden. Make it nullable; enforce required-ness at the consumer if needed.
3. **Removing a field** requires a deprecation event in the schema doc and a one-release-cycle grace period.
4. **Renaming a field** is forbidden. Add a new field, deprecate the old, dual-write during transition.
5. **Changing a field's type** is forbidden. Add a new field with a new name.
6. **Reordering fields** (in Avro/Protobuf) is forbidden — order is encoded by field number/position.

## Versioning

- **Event type names are versioned**: `TradeEvent.v1`, `TradeEvent.v2`. Consumers subscribe to a specific version.
- **`schemaVersion`** is an integer field on every event. Increment on any change.
- **`codeVersion`** is a git SHA recorded on derived events for traceability.

## Validation

Every event entering the system passes through `EventValidator`:

1. Required fields present.
2. Time fields in valid range (not `Instant.MIN`, not far-future).
3. `eventTime ≤ ingestTime + clockSkewTolerance`.
4. Numeric fields within sane bounds.
5. Reference data resolvable (instrument exists, exchange exists).

Validation failures emit a `ValidationFailedEvent` to a dead-letter topic with the rejection reason. They are **not** silently dropped.

## Testing

- **Contract tests** assert that every serialized event round-trips through the Java record without loss.
- **Compatibility tests** assert that old events deserialize under new schemas (backward compat) and new events deserialize under old schemas (forward compat) wherever required.
- **Golden file tests** keep one canonical example of every event type in `src/test/resources/golden/`. Changing the file requires a code review.
