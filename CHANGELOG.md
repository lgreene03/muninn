# Changelog

All notable changes to Muninn are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) (with pre-1.0 caveats described in [VERSIONING.md](docs/steering/VERSIONING.md)).

## [Unreleased]

### Added
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
