# Changelog

All notable changes to Muninn are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) (with pre-1.0 caveats described in [VERSIONING.md](docs/steering/VERSIONING.md)).

## [Unreleased]

### Added
- **Phase 8 — Trino-backed Query API.** `FeatureQueryBackend` abstraction in `io.muninn.query.backend` with two implementations: `DuckDbFeatureQueryBackend` (default; refactored from the inlined DuckDB SQL) and `TrinoFeatureQueryBackend` (new; reads Iceberg via `io.trino:trino-jdbc:434`). Backend selection is property-driven via `muninn.query.backend` (default `duckdb`; `production-reference` flips to `trino`). The `muninn.query.requests` and `muninn.query.latency` metrics now carry a `backend` tag. New ArchUnit rule `query_api_depends_only_on_backend_abstraction` enforces that the Query API surface depends only on the interface. Helm chart `deploy/helm/muninn/templates/deployment-query.yaml` plumbs the backend flag and Trino connection block into the container as `MUNINN_QUERY_*` env vars. See [ADR-0006](docs/adr/0006-trino-query-backend.md).
- **Phase 8 (scaffolded) — Production-reference architecture.**
  - Hardening pass on the Terraform modules (already on disk): MSK security group bounded to VPC CIDR (was `0.0.0.0/0`); MSK client-broker encryption set to `TLS` (was `TLS_PLAINTEXT`); CloudWatch broker log group with bounded retention; MSK enhanced monitoring at `PER_BROKER`. S3 warehouse bucket now ships with server-side AES256 encryption, versioning, lifecycle transitions (STANDARD_IA at 30d, GLACIER_IR at 90d, non-current expiry at 30d), and `allow_destroy` defaulted to false.
  - ADR-0003 (Managed Kafka via MSK), ADR-0004 (EKS over Fargate-only), ADR-0005 (Iceberg + Glue Data Catalog).
  - [`docs/steering/PHASE8_MIGRATION.md`](docs/steering/PHASE8_MIGRATION.md) — operational migration playbook (local → cloud), including dual-write Kafka cutover with the divergence detector as the safety check.
  - [`docs/DEPLOY.md`](docs/DEPLOY.md) — fresh-deploy walkthrough including cost expectations.
  - [`scripts/migrate-parquet-to-iceberg.sh`](scripts/migrate-parquet-to-iceberg.sh) — idempotent metadata-only conversion via pyiceberg + Glue.
  - `RUNBOOK.md` and `SECURITY_MODEL.md` updated with cloud-specific operational and hardening notes.
  - `READING_GUIDE.md` updated to surface the new docs to infrastructure / SRE readers.
- **Phase 4 — Replay engine.** Working end-to-end deterministic replay:
  - `ReplayJob` / `ReplayJobStatus` / `ReplayJobRegistry` — job lifecycle and in-memory state.
  - `ReplayJobRunner` — runs a fresh feature-engine instance over `ReplayEventSource`, routing outputs to the `.replay` sibling topic. Uses the same `FeatureEngineRunner` as the live path (one computation path).
  - `ReplayJobController` — `POST /api/v1/replay/jobs`, `GET /api/v1/replay/jobs/{id}`, `GET /api/v1/replay/jobs`.
  - `ShadowReplayComparator` — Kafka listener for live + replay topics; feeds matched pairs to the divergence detector.
  - `ReplayDeterminismIntegrationTest` — the proof: produces 6 trades, runs the engine live, replays the same range, asserts byte-identical outputs on the computational fields.
- `FeatureEngineRunner` gains a `mode` tag on every metric and a configurable topic resolver. Live wiring unchanged; replay wiring uses `"replay"` and `t -> t + ".replay"`.
- ADR-0002: `eventId` is provenance metadata, not computational output (documents the deliberate scope of the determinism claim and the divergence-detector field set).
- Type mapping for `FeatureComputedEvent` in `spring.json.type.mapping`.
- `features.vwap.1m.v1` and `features.vwap.1m.v1.replay` in `create-topics.sh` (the old `features.vwap.v1` entry didn't match `VwapComputer.FEATURE_NAME`).
- GitHub Actions CI workflow (`mvn verify` on push and PR) with JaCoCo report artifact and a separate smoke-test job that brings up `docker compose`.
- ArchUnit architectural rule test (`ArchitectureRulesTest`) enforcing seven rules drawn from the steering docs: no wall-clock reads in `feature.compute`, no `Random` in feature code, layering boundaries (`shared` is pure, `feature` doesn't depend on `ingestion`, `query` doesn't depend on `feature`), no field injection, no `printStackTrace`.
- JaCoCo coverage reporting in the verify phase, with a 25% bundle floor (regression guard) and per-package gates: `feature.compute` ≥ 95%, `shared.time` ≥ 90%.
- CI and license badges in the README.
- Steering document set covering principles, constraints, domain model, deterministic replay, schema strategy, testing, observability, storage, roadmap, agent workflow, coding standards, non-goals.
- OSS hygiene: Apache 2.0 license, CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md.
- GitHub templates: PR template and issue templates (bug, feature, question).
- Reading guide mapping roles to entry-point docs.
- A–Z glossary of project terms.
- ADR infrastructure with template and seed ADR (ADR-0001).
- Performance budgets, runbook, security threat model, and versioning policy.
- Sealed `MarketEvent` hierarchy: `TradeEvent`, `CandleEvent`, `OrderBookSnapshotEvent`, `FeatureComputedEvent`.
- UUIDv7 (RFC 9562) identifiers across all events.
- Typed exception hierarchy: `MuninnException` → `Validation/Ingestion/Storage/Replay`.
- Binance public WebSocket adapter (trades + depth20 book snapshots) for BTC-USDT.
- `EventValidator` with five rules; dead-letter routing for rejected events.
- Micrometer ingestion metrics: `events.total`, `validation.failed`, `source.latency`.
- Feature engine with `EventSource` abstraction (live + replay), `WindowManager`, `WatermarkTracker`, `CheckpointManager`.
- `VwapComputer` as the bootstrap pure-function feature.
- `FeatureParquetWriter` and `FeatureArchivalConsumer` for warm storage.
- Replay scaffolding: `ReplayService`, `ReplayConsumerFactory`, `ReplayDivergenceDetector`.
- Query API scaffolding: `FeatureQueryController`, `FeatureQueryService`.
- Flyway migrations for exchanges, instruments, seed reference data, feature definitions, event metadata.
- `scripts/smoke.sh` and `scripts/create-topics.sh` for end-to-end validation.
- 94 unit/contract/determinism/integration tests across 18 test files.
- Golden-file contract tests for trade and order-book events.
- Golden-dataset determinism test for VWAP.
- Testcontainers integration test (PostgreSQL + Kafka).

### Changed
- Initial event-modeling pass (`Event`, `EventEnvelope`, single `EventProducer`) replaced by typed sealed hierarchy and module-aligned producers (`MarketEventProducer`, `DeadLetterProducer`).
- Configuration migrated from `@Value` to `@ConfigurationProperties` throughout.
- Roadmap Phase 1 and Phase 3 marked complete; Phase 2 merged into Phase 1.

### Removed
- Legacy `Event` / `EventEnvelope` records (superseded by sealed `MarketEvent`).

## How to Update This File

Every PR that introduces a user-visible change adds an entry under `[Unreleased]`. At release time, that block is promoted to a versioned section dated `YYYY-MM-DD`. See [VERSIONING.md](docs/steering/VERSIONING.md) for the full release process.
