# GLOSSARY.md

A flat, alphabetical lookup of terms used throughout Muninn. For relationships between concepts, see [DOMAIN_MODEL.md](DOMAIN_MODEL.md).

---

**Adapter.** A module that connects to a specific external data source (e.g., one exchange) and normalizes its output into canonical `MarketEvent`s. Adapter code lives in `io.muninn.ingestion.adapter`.

**Append-only.** A property of the event log: records are added at the end and never modified or removed. The basis of [immutability](#immutability) and replayability.

**Avro.** A binary serialization format with strong schema-evolution semantics, used by Kafka ecosystems. Muninn's planned post-MVP wire format. See [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md).

**Backfill.** A bulk-historical processing run. In Muninn, backfill is a [replay](#replay) of an event range — not a separate code path.

**Batch.** A bounded set of events processed together. Muninn batches at IO boundaries (writes to Parquet) but processes events one at a time inside the [feature engine](#feature-engine).

**BigDecimal.** Java's arbitrary-precision decimal type. Required for any monetary or quantity field. Floating-point is forbidden for prices and sizes.

**Candle.** A time-bucketed OHLCV summary of trades. Modeled as `CandleEvent`. Can come from the exchange or be computed.

**Checkpoint.** A durable snapshot of [feature engine](#feature-engine) state at a known [watermark](#watermark). Allows [replay](#replay) to resume mid-stream without starting from `t=0`. See [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md).

**`cloud-cheap`.** One of four [deployment profiles](#deployment-profile). Single VPS, free-tier services. Suitable for a public demo.

**Compaction (file).** Background job that merges small Parquet files into larger ones per partition.

**Compaction (Kafka topic).** Kafka feature that keeps only the latest message per key. **Forbidden** on Muninn raw event topics — breaks replay.

**Contract test.** A test that asserts a public contract (event schema, API shape) round-trips correctly and remains compatible across versions. See [TESTING_STRATEGY.md](TESTING_STRATEGY.md).

**Dead letter.** A topic (`events.deadletter`) that receives events rejected by [validation](#validation), with a structured failure reason.

**Deployment profile.** A configuration set selecting infrastructure size and dependencies. Four profiles: `local-lite`, `local-full`, `cloud-cheap`, `production-reference`.

**Determinism.** The property that a computation produces identical outputs given identical inputs and code, on any machine, any time. Muninn's central invariant. See [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md).

**Divergence.** Any byte-level mismatch between an output produced by the live path and the same output produced by [replay](#replay). Always a defect, never expected.

**DuckDB.** An embedded analytical SQL engine that reads Parquet files directly. Muninn's query engine in MVP. See [DATA_STORAGE_STRATEGY.md](DATA_STORAGE_STRATEGY.md).

**Event.** An immutable, time-stamped record of a fact that occurred in the world. The atomic unit of Muninn.

**Event log.** The append-only sequence of every event ever observed. The single source of truth. Hosted in [Redpanda](#redpanda), archived to Parquet.

**Event source.** The abstraction that supplies events to the [feature engine](#feature-engine). Two implementations: live (Kafka consumer) and replay (historical reader). The engine cannot tell them apart.

**Event time.** The wall-clock time the event occurred in the world, as reported by the source. Used for windowing, ordering, and watermarks. Distinct from [processing time](#processing-time).

**Exchange.** A reference-data entity representing a data source (e.g., Coinbase). One exchange has many [instruments](#instrument).

**Feature.** A derived value computed from events. Output as `FeatureComputedEvent`.

**Feature engine.** The module that consumes events and produces [features](#feature). Stateful, deterministic, checkpointed. Lives in `io.muninn.feature`.

**Feature version.** The git SHA (or semantic-version string) of the engine code that produced a `FeatureComputedEvent`. Recorded on every output for traceability.

**Golden file / dataset.** A version-controlled canonical example of an event, feature output, or computation result. Used in contract and feature tests. Changing one requires explicit reviewer approval.

**Iceberg.** Apache Iceberg — a table format adding ACID and time-travel semantics over Parquet. Planned for the [`production-reference`](#deployment-profile) profile.

**Idempotent.** A property where running the same operation twice yields the same result as running it once. Required of Muninn's Kafka producers and recovery paths.

**Immutability.** The property that events, once written, are never modified or deleted. Corrections are themselves new events.

**Ingest lag.** Wall-clock seconds between when an event occurred at the source and when Muninn observed it. A named metric.

**Ingestion service.** The module that connects to external sources, validates events, and writes to the event log. See [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md).

**Instrument.** A trade-able symbol on an exchange (e.g., `BTC-USD`). Reference data in PostgreSQL.

**JSON.** The MVP wire format. Chosen for debuggability and local-first friction-reduction. See [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md).

**Late event.** An event whose [event time](#event-time) is below the current [watermark](#watermark) when it arrives. Routed to a configured policy: drop, side-output, or revise.

**Live path.** The pipeline from external source through ingestion, broker, and feature engine, in real time.

**`local-full`.** The full local stack including observability. Reference target for the Mac mini M4.

**`local-lite`.** The minimum viable local stack. Suitable for laptops and CI.

**MinIO.** S3-compatible object storage. Muninn's local Parquet store. Swaps for AWS S3 in production-reference.

**MarketEvent.** The abstract supertype of all market-data events.

**Muninn.** Old Norse for "memory." One of Odin's two ravens. The project's name.

**Non-goal.** An explicit exclusion. See [NON_GOALS.md](NON_GOALS.md).

**OHLCV.** Open, High, Low, Close, Volume — the standard fields of a [candle](#candle).

**OpenTelemetry.** A vendor-neutral observability framework. Muninn uses it for traces. See [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md).

**Order book snapshot.** A point-in-time picture of the resting bids and asks at an exchange. Modeled as `OrderBookSnapshotEvent`.

**Parquet.** A columnar file format. Muninn's archival format for historical events and computed features.

**Partition (Kafka).** A subdivision of a Kafka topic, preserving order within itself.

**Partition (Parquet).** A directory structure that groups Parquet files by field values (e.g., `year=2026/month=05/day=11`), enabling predicate pruning.

**Phase.** A milestone in the [roadmap](ROADMAP.md). Phases are not skipped.

**PostgreSQL.** Muninn's metadata store. Never holds event data.

**Processing time.** The wall-clock time at which the system observed or processed an event. Used for SLAs and lag metrics; never for feature logic.

**`production-reference`.** The aspirational scaled deployment profile. Kafka, Iceberg, Trino, Kubernetes.

**Protobuf.** A binary serialization format. Considered and not chosen for MVP. See [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md).

**Pure function.** A function whose output depends only on its inputs, with no side effects. Required of all feature-computation code.

**Query API.** The read-only HTTP service that serves feature time-series and metadata. See [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md).

**Record (Java).** A Java language construct for immutable data classes. Muninn's preferred domain type.

**Redpanda.** A Kafka-compatible broker. Muninn's MVP event-log implementation.

**Replay.** Re-executing the [feature engine](#feature-engine) over historical events to reproduce its outputs. See [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md).

**Replay engine.** The module that schedules and executes replay jobs.

**Replay job.** A configured replay: time range, topics, feature version, output sink.

**Rollover.** The process of archiving event-log segments from Redpanda to Parquet in MinIO once they age past the hot-retention window.

**Schema evolution.** The rules governing how event schemas may change without breaking consumers. See [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md).

**Schema version.** An integer field on every event. Incremented on any schema change.

**Shadow replay.** A continuous replay running in parallel with the live path, used for [divergence](#divergence) detection.

**`shared-schema`.** The Muninn module defining canonical event records and validators. Pure library; depends on no other Muninn module.

**Smoke test.** An end-to-end happy-path test. `./scripts/smoke.sh` is the canonical script.

**SLO.** Service Level Objective — a measurable target on a metric (e.g., "p99 feature latency < 500 ms over 1h").

**Source.** The origin of an event. Recorded as the `source` field; used for partitioning and provenance.

**Steering doc.** A document under `docs/steering/` describing an architectural decision or constraint. Authoritative.

**Testcontainers.** A Java library for spinning up real infrastructure (Kafka, Postgres, MinIO) in tests. Muninn's default integration-test mechanism.

**Topic.** A named stream in Redpanda (e.g., `events.trade`, `features.vwap.v1`).

**Trade event.** A single executed trade reported by an exchange. Modeled as `TradeEvent`.

**Tumbling window.** A fixed-size, non-overlapping [feature window](#feature-window) (e.g., one-minute bars).

**Sliding window.** A fixed-size window that advances by a smaller stride, producing overlapping windows.

**Session window.** A window defined by gaps in activity rather than fixed boundaries.

**UUIDv7.** A time-ordered UUID variant. Muninn's identifier format for all events.

**Validation.** The schema and semantic checks applied at ingestion before an event enters the log. Failures go to the [dead letter](#dead-letter) topic.

**VWAP.** Volume-Weighted Average Price. A common bootstrap feature.

**Watermark.** A monotonic estimate that "we have seen all events with event-time ≤ W." Windows close when their end is below the watermark. See [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md).

**Window.** A bounded event-time range over which a feature is computed. See [FeatureWindow in DOMAIN_MODEL.md](DOMAIN_MODEL.md).
