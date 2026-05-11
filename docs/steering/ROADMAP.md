# ROADMAP.md

Phased delivery. Each phase ends with a working, tested, documented increment. Phases are not skipped.

## Phase 0 — Steering Docs and Repo Skeleton ✅ (in progress)

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

## Phase 1 — Local Ingestion

**Goal.** A single exchange adapter writes validated events to Redpanda.

**Deliverables.**
- `ingestion-service` module with a Coinbase (or equivalent) public WebSocket adapter.
- `MarketEvent` validation pipeline.
- Dead-letter handling.
- Ingestion metrics ([OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md)).
- Integration test: real WebSocket (recorded), real Redpanda (Testcontainers), validated event in topic.
- One instrument (`BTC-USD`).

**Exit criteria.** `./scripts/smoke.sh` produces real exchange events in `events.trade` and validation metrics in Prometheus.

---

## Phase 2 — Canonical Events

**Goal.** Stable, versioned event schemas for all initial event types.

**Deliverables.**
- `shared-schema` module with records for `TradeEvent`, `CandleEvent`, `OrderBookSnapshotEvent`, `FeatureComputedEvent`.
- Jackson configuration and round-trip tests.
- Golden file tests in `src/test/resources/golden/`.
- `EventValidator` with full coverage.
- Schema version field on every event.
- Adapter normalization tested against recorded exchange payloads.

**Exit criteria.** Every event in Redpanda conforms to a canonical schema. Contract tests prevent silent drift.

---

## Phase 3 — Feature Engine

**Goal.** A deterministic feature computation engine over the live stream.

**Deliverables.**
- `feature-engine` module with the `EventSource` abstraction (live + replay implementations).
- Watermark logic, late-event policies.
- A small set of bootstrap features: 1-minute candle, rolling VWAP, trade rate.
- Feature outputs to Redpanda (`features.<name>.v<n>`) and Parquet rollover to MinIO.
- Checkpoint write/restore.
- Feature metrics.
- Determinism unit tests (same-JVM replay).

**Exit criteria.** Live ingestion drives feature output. Engine restart resumes from checkpoint with no data loss or double counting.

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
