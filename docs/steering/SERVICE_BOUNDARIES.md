# SERVICE_BOUNDARIES.md

Muninn is composed of a small number of well-bounded services. Each one is independently deployable but begins life as a Spring Boot module within a single monorepo and a single JAR. Splitting into separate processes happens at Phase 5+, when boundaries have stabilized.

## Module Map

```
muninn/
├── ingestion-service/   # Exchange adapters -> event log
├── feature-engine/      # Event log -> derived features
├── replay-engine/       # Historical replay over the event log
├── query-api/           # Read-side: serves features and metadata
├── shared-schema/       # Event types, validators, constants
└── local-infra/         # Docker Compose, Terraform stubs, scripts
```

---

## ingestion-service

**Responsibility.** Connect to external data sources (initially crypto-exchange public WebSocket and REST APIs), normalize incoming records into canonical `MarketEvent`s, validate them, and append them to the Redpanda event log.

**Inputs.**
- Exchange WebSocket/REST feeds (one adapter per exchange).
- HTTP `POST /api/v1/events` for synthetic or test events.
- Configuration: exchange credentials (read-only), instrument list, backoff policy.

**Outputs.**
- Validated `MarketEvent`s to the appropriate Redpanda topic (`events.trade`, `events.book.snapshot`, etc.).
- Rejected events to `events.deadletter` with a structured failure reason.
- Metrics: ingest rate, validation failure rate, source latency, reconnect counter.

**Dependencies.**
- Redpanda (producer).
- PostgreSQL (instrument and exchange reference data only — read).
- Outbound network (exchange APIs).

**Must not.**
- Compute derived features.
- Mutate events.
- Persist anything other than the validated event to Redpanda.
- Read from the event log (it is write-only here).

---

## feature-engine

**Responsibility.** Consume events from Redpanda, apply registered feature definitions, and emit `FeatureComputedEvent`s. Maintain feature-engine state, checkpoint it, and tolerate restarts.

**Inputs.**
- `MarketEvent` streams from Redpanda.
- Feature definitions from PostgreSQL.
- (At restart) the most recent checkpoint from MinIO.

**Outputs.**
- `FeatureComputedEvent`s to Redpanda (`features.<name>.v<n>`).
- Periodic state checkpoints to MinIO.
- Metrics: events processed, watermark lag, feature latency, late-event counter, checkpoint duration.

**Dependencies.**
- Redpanda (consumer + producer).
- MinIO (checkpoint store).
- PostgreSQL (feature definitions).

**Must not.**
- Read the wall clock inside feature computation.
- Call external services inside feature computation.
- Behave differently for live vs replay sources.

---

## replay-engine

**Responsibility.** Execute `ReplayJob`s — re-run the feature engine over a specified event-time range, writing outputs to a designated sink (Parquet in MinIO, or a Redpanda topic for divergence comparison).

**Inputs.**
- `ReplayJob` specifications from PostgreSQL.
- Historical events from Redpanda (seek by timestamp) or from Parquet in MinIO (after retention rollover).
- Checkpoints from MinIO.

**Outputs.**
- Replayed `FeatureComputedEvent`s to a job-specific topic or Parquet directory.
- Job-status updates to PostgreSQL.
- Divergence metrics when running in shadow mode.

**Dependencies.**
- The **same** feature-engine module the live path uses (compiled-in, not duplicated).
- Redpanda (consumer).
- MinIO (Parquet read + write).
- PostgreSQL (job state).

**Must not.**
- Re-implement feature logic. It uses the same code as `feature-engine`.
- Mutate the event log.
- Run without an explicit `featureVersion` and `from`/`to` range.

---

## query-api

**Responsibility.** Read-only HTTP API for clients (dashboards, notebooks, external tools). Serves feature time-series, replay-job status, event-log metadata, and ad-hoc analytical queries via DuckDB over the Parquet warehouse.

**Inputs.**
- HTTP requests.

**Outputs.**
- JSON responses (paginated, capped).
- Metrics: query rate, query latency p50/p95/p99, cache hit rate.

**Dependencies.**
- DuckDB (analytical queries over MinIO Parquet).
- PostgreSQL (metadata, replay-job status, feature definitions).
- Redpanda (only for very recent data not yet rolled to Parquet, if applicable).

**Must not.**
- Write to the event log.
- Trigger feature computation.
- Expose mutation endpoints (those belong elsewhere, if at all).

---

## shared-schema

**Responsibility.** The canonical Java module defining every domain type ([DOMAIN_MODEL.md](DOMAIN_MODEL.md)), every event record, every validator, and every Jackson configuration. Consumed by all other modules.

**Inputs.** None — it is a pure library.

**Outputs.** A JAR.

**Dependencies.** Java standard library, Jackson, Hibernate Validator. Nothing service-specific.

**Must not.**
- Depend on any other Muninn module.
- Contain business logic (compute features, route events).
- Carry runtime configuration.

---

## local-infra

**Responsibility.** Everything required to run Muninn locally and (eventually) deploy a `cloud-cheap` profile: `docker-compose.yml`, Compose overlays per profile, MinIO bucket bootstrap, PostgreSQL migrations bootstrap, observability stack composition, smoke-test scripts, and Terraform stubs for the `production-reference` profile.

**Inputs.** None at runtime.

**Outputs.** Working local environment, deployable artifacts.

**Dependencies.** Docker, Compose, Terraform (stubs only in MVP).

**Must not.**
- Contain application code.
- Depend on application modules.

---

## Inter-Module Communication

In the MVP, all modules run inside a single Spring Boot process and communicate via direct method calls through well-defined interfaces. When modules are extracted into separate processes (Phase 5+), the **only** communication channel is Redpanda — no synchronous RPC between services, except for `query-api` reading from PostgreSQL and DuckDB.

This rule keeps the system event-native end-to-end.

## Proposing a New Module

Before creating a new module:

1. State its responsibility in one sentence.
2. Demonstrate that no existing module can absorb it without violating its boundary.
3. Confirm it does not introduce a synchronous dependency between event-producing services.
4. Open a PR that updates this document **before** the first line of module code.
