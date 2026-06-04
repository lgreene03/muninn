# Changelog

All notable changes to Muninn are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) (with pre-1.0 caveats described in [VERSIONING.md](docs/steering/VERSIONING.md)).

## [Unreleased]

### Added
- **Phase 10 — Live feature streaming (SSE).** New `GET /api/v1/features/stream` (`text/event-stream`) pushes each `FeatureComputedEvent` to connected clients as the engine produces it — a push alternative to polling the historical Query API. Optional `?feature=<name>` filters to a single feature. A single `FeatureStreamConsumer` pattern-subscribes to `features\..*` on a unique per-process consumer group (from `latest`, never committing — a live tail, not a replayable cursor) and `FeatureStreamBroker` fans each event out in memory, so Kafka load is constant in the number of connected clients. Served by Spring MVC `SseEmitter` (no reactive runtime added). Config under `muninn.streaming.*` (`enabled`, `poll-timeout`, `emitter-timeout`, `keepalive-interval`; `topic-pattern` defaults to `features\..*`); when `enabled: false` the route stays up (keepalive-only). New metrics `muninn.streaming.{subscriptions.active,events.received,messages.sent,disconnects}` (tagged `endpoint=features`). New ArchUnit rule `streaming_does_not_depend_on_feature_ingestion_or_query` (11 rules total). This is the server-side delivery of cross-repo trigger **T3**. See [ADR-0009](docs/adr/0009-streaming-features-sse.md).
- **Cross-stack smoke test.** `scripts/smoke-stack.sh` + `docker-compose.stack.yml` validate the full Norse pipeline end-to-end: Trade → Muninn → Huginn (OBI strategy) → Sleipnir (sim fill) → Portfolio update. Supports `--teardown` flag for CI use.

### Changed
- **Spring Boot 3.4.1 → 3.5.14** (latest maintenance release; Flyway 11.7, Kafka client 3.9, Micrometer 1.15).
- **Springdoc 2.8.5 → 2.8.17** (latest 2.x for Spring Boot 3.x).

### Previously added
- **Phase 8 — Multi-exchange adapter framework.** `IngestionPipeline` now consumes `List<ExchangeAdapter>` injected by Spring; each enabled adapter is registered as its own bean via `IngestionAdapterConfiguration` with `@ConditionalOnProperty` on `muninn.ingestion.<name>.enabled`. Adding a third exchange is a documented 5-file change with no edits to the pipeline. Per-source metric tagging — `muninn.ingest.events.total`, `validation.failed`, `source.latency`, `lag.seconds` all carry a `source` label matching the adapter's `source()`. New `CoinbaseConfig` mirrors `BinanceConfig` as the reference second source. New ArchUnit rule `ingestion_pipeline_depends_only_on_adapter_interface` (10 rules total) enforces that only the configuration class sees concrete adapter classes. Helm chart plumbs `ingestion.binance.*` and `ingestion.coinbase.*` blocks through `deployment-ingestion.yaml` as `MUNINN_INGESTION_*` env vars; Coinbase defaults to `enabled: false` so Phase 1 behaviour is preserved. Five-test unit suite covers multi-source dispatch, empty-list idleness, per-source metric tags, dead-letter routing under the right source, and shutdown-with-throwing-adapter resilience. See [ADR-0008](docs/adr/0008-multi-exchange-adapter-framework.md).
- **Phase 7 — Docs and demo polish.** README gains a 30-second hook + a "Read more" block pointing at the new artifacts. New files: [`docs/DEMO.md`](docs/DEMO.md) — runnable 10-minute walkthrough that boots the stack, sends trades, runs a replay, and observes zero divergence; [`docs/demo/screencast-outline.md`](docs/demo/screencast-outline.md) — 5-minute screencast shot list + narration draft; [`docs/blog/2026-05-18-deterministic-replay.md`](docs/blog/2026-05-18-deterministic-replay.md) — long-form post on the central technical claim and how it's enforced; [`docs/talks/2026-deterministic-replay-talk.md`](docs/talks/2026-deterministic-replay-talk.md) — conference-talk abstract + outline ("One Computation Path"). ROADMAP Phase 7 flipped to mostly-complete; remaining items (recorded screencast, screenshots, `cloud-cheap` profile) tracked.
- **Phase 8 — Iceberg-backed archival sink.** `FeatureSink` abstraction in `io.muninn.storage.sink` with two implementations: `ParquetFeatureSink` (default; wraps the existing `FeatureParquetWriter`) and `IcebergFeatureSink` (new; appends rows to Iceberg tables via the AWS Glue catalog, with a HadoopCatalog escape hatch for tests / non-AWS use). Sink selection is property-driven via `muninn.archival.sink` (default `parquet`; `production-reference` flips to `iceberg`). The Iceberg sink uses the same `features_<name>_<version>` table naming as `TrinoFeatureQueryBackend`, so writes and reads agree end-to-end. New ArchUnit rule `archival_consumer_depends_only_on_sink_abstraction` enforces that `FeatureArchivalConsumer` depends only on the interface. Helm chart plumbs the flag and the catalog config into `deployment-feature.yaml` as `MUNINN_ARCHIVAL_*` env vars (Iceberg vars only set when `sink=iceberg`). New deps: `org.apache.iceberg:iceberg-core/data/parquet/aws:1.6.1`. See [ADR-0007](docs/adr/0007-iceberg-feature-sink.md).
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
