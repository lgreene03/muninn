# ROADMAP.md

Phased delivery. Each phase ends with a working, tested, documented increment. Phases are not skipped.

## Phase 0 — Steering Docs and Repo Skeleton ✅

**Goal.** Establish the conceptual and structural foundation before writing application code.

**Deliverables.**
- This document set (`docs/steering/*.md`).
- `AGENTS.md`, `README.md`.
- Empty package skeleton matching [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md).
- `docker-compose.yml` for the `local-lite` profile.
- CI bootstrap (build + lint + unit tests).
- Spotless and ArchUnit baselines.

**Exit criteria.** A new contributor (or AI agent) can read the docs, run `docker-compose up -d`, and see the empty services start and report `UP` from `/actuator/health`.

---

## Phase 1 — Local Ingestion + Canonical Events ✅

**Goal.** A single exchange adapter writes validated, typed events to Redpanda.

**Deliverables.**
- `ingestion-service` with Binance public WebSocket adapter (trades + order book snapshots).
- `shared-schema` with sealed `MarketEvent` hierarchy: `TradeEvent`, `CandleEvent`, `OrderBookSnapshotEvent`, `FeatureComputedEvent`.
- `EventValidator` with 5 validation rules from EVENT_SCHEMA_STRATEGY.md.
- Dead-letter routing for rejected events.
- Ingestion metrics (Micrometer → Prometheus): `muninn.ingest.events.total`, `muninn.ingest.validation.failed`, `muninn.ingest.source.latency`.
- Golden-file contract tests preventing silent schema drift.
- Binance parser unit tests with recorded exchange payloads.
- UUIDv7 (RFC 9562) for time-ordered event IDs.
- Typed exception hierarchy (`MuninnException` → `ValidationException`, `IngestionException`, `StorageException`, `ReplayException`).
- Flyway migrations for exchange/instrument reference data (PostgreSQL).
- `@ConfigurationProperties` throughout (no `@Value` injection).
- Testcontainers integration test (PostgreSQL + Kafka).
- `scripts/smoke.sh` and `scripts/create-topics.sh`.
- One instrument (`BTC-USDT` via Binance).
- 46 unit tests, 4 integration tests.

**Exit criteria.** `./scripts/smoke.sh` produces real exchange events in `events.trade` and validation metrics in Prometheus.

---

## Phase 2 — _(Merged into Phase 1)_

The canonical events and schema work originally planned for Phase 2 was delivered as part of Phase 1.
The sealed `MarketEvent` hierarchy, `EventValidator`, golden-file contract tests, and schema versioning
are all in place. See Phase 1 deliverables above.

---

## Phase 3 — Feature Engine ✅

**Goal.** A deterministic feature computation engine over the live stream.

**Deliverables.**
- `feature-engine` module with the `EventSource` abstraction (live + replay implementations).
- Watermark logic, late-event policies.
- A small set of bootstrap features: rolling VWAP.
- Feature outputs to Redpanda (`features.<name>.v<n>`) and Parquet rollover to MinIO.
- Checkpoint write/restore using raw event buffers for deterministic rehydration.
- Feature metrics.
- Determinism unit tests (same-JVM replay).
- Spring `SmartLifecycle` management for graceful startup/shutdown.

**Exit criteria.** Live ingestion drives feature output. Engine restart resumes from checkpoint with no data loss or double counting. (Met: `smoke.sh` validates end-to-end flow).

---

## Phase 4 — Replay Engine

**Goal.** Re-execute the feature engine over historical events, producing outputs identical to the live path.

**Deliverables.**
- `replay-engine` module that submits and tracks `ReplayJob`s.
- Replay sources: Redpanda (seek-by-timestamp) and Parquet (via DuckDB).
- Shadow-replay comparator and `muninn.replay.divergence` metrics.
- Cross-JVM and checkpointed replay tests.
- Nightly divergence audit job.

**Exit criteria.** A replay of yesterday's events produces byte-identical outputs to the live run. Divergence dashboard is green.

---

## Phase 5 — Query API

**Goal.** A read-only HTTP API for clients.

**Deliverables.**
- `query-api` module: feature time-series endpoint, replay-job status endpoint, event-metadata endpoint.
- DuckDB-over-MinIO query path.
- OpenAPI specification.
- Query metrics.
- Pagination, request validation, structured error responses.

**Exit criteria.** A dashboard or notebook can fetch any feature for any historical range. Latency is within budget.

---

## Phase 6 — Observability

**Goal.** Full telemetry stack and operational dashboards.

**Deliverables.**
- Prometheus + Grafana + Tempo (or Jaeger) in `local-full`.
- All metrics from [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md) emitted.
- Three Grafana dashboards committed.
- Alert rules.
- Trace correlation: an `eventId` traceable from ingestion through feature emission.

**Exit criteria.** Operating the system from telemetry is possible end-to-end. The pipeline-overview dashboard tells the truth about system health.

---

## Phase 7 — Docs and Demo Polish

**Goal.** The repository is presentable as a portfolio artifact.

**Deliverables.**
- Polished `README.md` with screenshots.
- A short demo script and recorded walkthrough.
- ADRs (Architecture Decision Records) for non-obvious choices.
- Contributor guide.
- `cloud-cheap` profile that runs on a single small VPS.

**Exit criteria.** A senior engineer reading the repo for 30 minutes can explain the architecture without help.

---

## Phase 8 — Production-Reference Architecture

**Goal.** Demonstrate the scaled-up topology without losing the local-first reality.

**Deliverables.**
- Terraform modules for AWS (or GCP) targeting Kafka (MSK or Redpanda Cloud), S3 + Iceberg, Trino, EKS.
- Helm charts for each service.
- Kafka cluster configuration and migration guide.
- Iceberg catalog setup and Parquet → Iceberg migration script.
- Trino deployment replacing DuckDB in the query path.
- Multi-exchange adapter framework.

**Exit criteria.** A reader can choose to deploy Muninn at small scale on a managed cloud, with the same code that runs locally.

---

## Out-of-Roadmap (Explicit)

The following are **never** roadmap items:

- Trading logic, order routing, execution.
- Price prediction models as product features.
- Multi-tenant SaaS.
- A commercial offering.

See [NON_GOALS.md](NON_GOALS.md).
