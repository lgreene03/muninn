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

## Phase 4 — Replay Engine ✅

**Goal.** Re-execute the feature engine over historical events, producing outputs identical to the live path.

**Deliverables.**
- `replay-engine` module that submits and tracks `ReplayJob`s.
- Replay source via Redpanda seek-by-timestamp (`ReplayEventSource`). _Parquet-via-DuckDB source deferred until events age past broker retention._
- HTTP API: `POST /api/v1/replay/jobs`, `GET /api/v1/replay/jobs/{id}`, `GET /api/v1/replay/jobs`.
- `ShadowReplayComparator` listens to live and replay topics, feeds the `ReplayDivergenceDetector`. Metric `muninn.replay.divergence.detected` fires on mismatch.
- `mode` tag on every feature-engine metric distinguishes live from replay.
- `ReplayDeterminismIntegrationTest`: the load-bearing proof — produces 6 trades, runs the engine live, submits a replay job for the same range, asserts byte-identical outputs on the computational fields. _Cross-JVM and checkpointed replay variants deferred to a follow-up._
- ADR-0002 records the deliberate exclusion of `eventId` from the determinism claim (provenance metadata, not computational output).
- _Nightly divergence audit cron job: deferred to Phase 6 (observability)._

**Exit criteria.** A replay of just-produced events produces byte-identical outputs to the live run, asserted by an integration test in CI. _Met._

---

## Phase 5 — Query API ✅

**Goal.** A read-only HTTP API for clients.

**Deliverables.**
- `query-api` module: feature time-series endpoint (`GET /api/v1/features/{featureName}`), replay-job status endpoint, and unified exception mapping.
- DuckDB-over-MinIO partition-pruned S3 query path.
- Standardized OpenAPI specification (`/api-docs` and `/swagger-ui.html`) fully integrated with Springdoc 2.8.5.
- Robust validation and global HTTP exception mappings using `QueryExceptionHandler`.

**Exit criteria.** A dashboard or notebook can fetch any feature for any historical range. Latency is within budget. (Met: `./scripts/smoke.sh` E2E Query API validation successful).

---

## Phase 6 — Observability ✅

**Goal.** Full telemetry stack and operational dashboards.

**Deliverables.**
- Prometheus + Grafana + Tempo (or Jaeger) in `local-full`.
- All metrics from [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md) emitted.
- Three Grafana dashboards committed.
- Alert rules.
- Trace correlation: an `eventId` traceable from ingestion through feature emission.

**Exit criteria.** Operating the system from telemetry is possible end-to-end. The pipeline-overview dashboard tells the truth about system health.

---

## Phase 7 — Docs and Demo Polish 🟢 _Mostly complete_

**Goal.** The repository is presentable as a portfolio artifact.

**Delivered.**
- ✅ Polished `README.md` with the 30-second hook and links to demo / blog / talk.
- ✅ [`docs/DEMO.md`](DEMO.md) — runnable 10-minute walkthrough that boots the stack, sends trades, runs a replay, and observes zero divergence.
- ✅ [`docs/demo/screencast-outline.md`](../demo/screencast-outline.md) — 5-minute screencast shot list + narration.
- ✅ [`docs/blog/2026-05-18-deterministic-replay.md`](../blog/2026-05-18-deterministic-replay.md) — long-form post on the central technical claim and how it's enforced.
- ✅ [`docs/talks/2026-deterministic-replay-talk.md`](../talks/2026-deterministic-replay-talk.md) — conference-talk abstract + outline ("One Computation Path").
- ✅ Seven ADRs covering the non-obvious architectural choices (Iceberg, Trino, MSK, EKS, eventId scope, ADR process, sink/backend abstractions).
- ✅ `CONTRIBUTING.md` is in place and current.

**Outstanding for full Phase 7 close:**
- 🟡 Actual recorded screencast (the outline is ready; recording is a one-shot creator task).
- 🟡 README screenshots — placeholder text references them; needs a Grafana panel capture once the observability stack has produced data on a long-running instance.
- ✅ `cloud-cheap` Compose overlay (`docker-compose.cloud-cheap.yml`). Drops Redpanda heap from 512 M → 256 M, adds OOM-safe memory caps to all services, profile-gates `redpanda-console`, and adds `restart: unless-stopped`. Targets a 4 GB / 2 vCPU single-VPS host. Usage: `docker compose -f docker-compose.yml -f docker-compose.cloud-cheap.yml up -d`.

**Exit criteria.** A senior engineer reading the repo for 30 minutes can explain the architecture without help. _Substantively met by the docs above; full close pending the screencast asset._

---

## Phase 8 — Production-Reference Architecture 🟡 _Scaffolded_

**Goal.** Demonstrate the scaled-up topology without losing the local-first reality.

**Delivered.**
- ✅ Terraform modules for AWS: `vpc`, `eks`, `msk`, `s3_iceberg`, `trino` under `local-infra/terraform/aws/modules/`.
- ✅ Helm chart for all four services (ingestion, feature, replay, query) under `deploy/helm/muninn/`, with HPA and Ingress.
- ✅ Hardened defaults: MSK security group bounded to VPC CIDR; TLS-only client-broker; S3 server-side encryption + versioning + lifecycle; `force_destroy` defaulted off.
- ✅ ADR-0003 (Managed Kafka via MSK), ADR-0004 (EKS over Fargate-only), ADR-0005 (Iceberg + Glue Data Catalog).
- ✅ [PHASE8_MIGRATION.md](PHASE8_MIGRATION.md) — operational migration playbook (local → cloud).
- ✅ [DEPLOY.md](../DEPLOY.md) — fresh-deploy walkthrough.
- ✅ `scripts/migrate-parquet-to-iceberg.sh` — metadata-only conversion via pyiceberg + Glue.
- ✅ RUNBOOK and SECURITY_MODEL updated with cloud-specific operational and hardening notes.

**Deferred application work** (tracked for sequential pickup):
- ✅ **Trino-backed Query API.** `FeatureQueryBackend` abstraction with `DuckDb*` and `Trino*` implementations, profile-switched via `muninn.query.backend` (default `duckdb`; `production-reference` flips to `trino`). ArchUnit enforces that the controller depends only on the abstraction. Helm chart wires it through. See [ADR-0006](../adr/0006-trino-query-backend.md).
- ✅ **Iceberg writer in the JVM application.** `FeatureSink` abstraction with `Parquet*` and `Iceberg*` implementations, profile-switched via `muninn.archival.sink`. The Iceberg sink uses the same `features_<name>_<version>` table convention from ADR-0006 so the Trino read path finds what this writes. ArchUnit enforces the consumer depends only on the abstraction. Helm chart wires it through. See [ADR-0007](../adr/0007-iceberg-feature-sink.md).
- ✅ **Multi-exchange adapter framework.** `IngestionPipeline` injects `List<ExchangeAdapter>` and starts every enabled adapter; per-exchange `@ConditionalOnProperty` wiring in `IngestionAdapterConfiguration` keeps adapter construction out of the pipeline. Per-source metric tags, lag clocks, and counters. ArchUnit enforces that only the configuration class sees concrete adapters. Coinbase is the reference second source; adding a third is a documented 5-file change. See [ADR-0008](../adr/0008-multi-exchange-adapter-framework.md).

**Exit criteria.** A reader can run `terraform apply` then `helm install` per [DEPLOY.md](../DEPLOY.md) and get a running cloud deployment. _Met for the scaffolded path._ The application-side cloud features (Iceberg writes, Trino-backed queries, multi-exchange) require the deferred work above.

---

## Phase 9 — Quantitative Research Infrastructure ✅

**Goal.** Demonstrate advanced mechanical sympathy and quantitative engineering capabilities, making the repository an exceptional portfolio piece for HFT/Quant roles.

**Deliverables.**
- `OrderBookL3` engine for deterministic, zero-allocation Market-by-Order depth reconstruction.
- `SPSCRingBuffer` implementing the LMAX Disruptor pattern with cache-line padding for ultra-low latency event dispatch.
- Advanced microstructural features: `OrderBookImbalanceComputer`, `MicroPriceComputer`, and `VPINComputer`.
- `SignalEvaluator` harness to compute Information Coefficient (IC) and backtest statistical alpha without executing trades.

**Exit criteria.** The system supports advanced quant features while maintaining its core local-first and determinism constraints. (Met: 132/132 unit tests passing).

---

## Out-of-Roadmap (Explicit)

The following are **never** roadmap items:

- Trading logic, order routing, execution.
- Price prediction models as product features.
- Multi-tenant SaaS.
- A commercial offering.

See [NON_GOALS.md](NON_GOALS.md).
