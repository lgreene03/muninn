# Muninn

**Event-native research infrastructure for deterministic replay, reproducible streaming analytics, and market-data feature computation.**

> *Muninn* — Old Norse for "memory." One of Odin's two ravens. The other is *Huginn* (thought), reserved for a possible companion project.

---

## The Problem

Quantitative research and streaming analytics suffer from a chronic correctness gap: **the system that develops a feature is rarely the system that runs it in production.** Research notebooks run on cleaned CSVs; production runs on a live stream; the two diverge silently. Backtests pass, deployments fail, and nobody can reproduce what happened yesterday.

Muninn closes that gap by making **one immutable event log the source of truth** and **one computation path serve both live and historical workloads.** The same feature engine that emits a value as events arrive emits an identical value when replaying yesterday's events — bit-for-bit.

If your system can't be replayed, it can't be debugged, audited, or improved with confidence. Muninn treats replay as an architectural invariant, not a feature.

---

## Architecture Overview

```
                    +-------------------+
                    | Exchange feeds    |  one adapter, one or two
                    +---------+---------+    instruments (MVP)
                              |
                              v
                    +-------------------+
                    | ingestion-service |  validate, normalize
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    |     Redpanda      |  immutable event log
                    +---------+---------+   (Kafka-compatible)
                              |
                  +-----------+-----------+
                  |                       |
                  v                       v
        +-----------------+      +-----------------+
        |  feature-engine |      |  replay-engine  |   same code,
        |    (live)       |      |   (historical)  |   different source
        +--------+--------+      +--------+--------+
                 |                        |
                 +------------+-----------+
                              |
                              v
                    +-------------------+
                    |  Parquet / MinIO  |   warm archive
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    |     DuckDB        |   embedded analytics
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    |    query-api      |   read-only HTTP
                    +-------------------+
```

**Metadata** (feature definitions, replay-job status, instrument reference data) lives in PostgreSQL. **Observability** (OpenTelemetry + Micrometer → Prometheus, Grafana, Tempo) is wired in from day one.

See [docs/steering/](docs/steering/) for the full architecture set, especially [DETERMINISTIC_REPLAY.md](docs/steering/DETERMINISTIC_REPLAY.md).

---

## Local-First Promise

The full system runs on a single **Mac mini M4 with 24 GB RAM** under Docker Compose. No managed cloud services. No credit card. No Kubernetes. The MVP boots in under 5 minutes from cold start.

Four deployment profiles share one codebase:

| Profile               | Target                                        |
|-----------------------|-----------------------------------------------|
| `local-lite`          | Laptop / CI — minimum viable pipeline         |
| `local-full`          | Mac mini M4 — full stack with observability   |
| `cloud-cheap`         | Single VPS — free-tier deployable             |
| `production-reference`| Cloud-scale topology (Phase 8 — documented)   |

See [LOCAL_FIRST_CONSTRAINTS.md](docs/steering/LOCAL_FIRST_CONSTRAINTS.md).

---

## MVP Scope

- **One exchange adapter** (e.g., Coinbase or Binance public WebSocket).
- **One or two instruments** (e.g., `BTC-USD`).
- **Canonical events**: `TradeEvent`, `CandleEvent`, `OrderBookSnapshotEvent`, `FeatureComputedEvent`.
- **Deterministic feature engine** with checkpoints and watermark-based windowing.
- **Replay engine** that reproduces live outputs byte-for-byte from the event log.
- **Read-only query API** over DuckDB + Parquet.
- **Observability stack**: Prometheus + Grafana + Tempo, with named application metrics.

See [ROADMAP.md](docs/steering/ROADMAP.md) for the phased delivery plan.

---

## Quickstart (placeholder)

> The application code is being built out in phases. The steps below describe the intended developer experience once Phase 1 lands.

```bash
git clone https://github.com/your-org/muninn.git
cd muninn

# Bring up the local-full stack
docker-compose up -d --wait

# Run the smoke test
./scripts/smoke.sh

# Dashboards
open http://localhost:3000          # Grafana
open http://localhost:8088          # Redpanda Console
open http://localhost:9001          # MinIO Console
open http://localhost:8080/swagger  # Query API
```

A new contributor or AI agent should be able to read `AGENTS.md`, run the commands above, and see a green smoke test.

---

## Roadmap

- **Phase 0** — Steering docs and repo skeleton _(in progress)_
- **Phase 1** — Local ingestion
- **Phase 2** — Canonical events
- **Phase 3** — Feature engine
- **Phase 4** — Replay engine
- **Phase 5** — Query API
- **Phase 6** — Observability
- **Phase 7** — Docs and demo polish
- **Phase 8** — Production-reference architecture

Detail in [ROADMAP.md](docs/steering/ROADMAP.md).

---

## Non-Goals

Muninn is **not**:

- A trading bot, an HFT engine, or an autonomous execution system.
- A source of financial advice or trading signals as a product feature.
- A crypto project (crypto APIs are the initial free data source — that is all).
- A production trading system.
- A Kubernetes-native or cloud-native MVP.
- Multi-exchange or multi-tenant in MVP.

Full statement: [NON_GOALS.md](docs/steering/NON_GOALS.md).

---

## Repo Status

**Phase 0.** Steering documentation complete. Initial Java scaffolding and Docker Compose in place. Phase 1 (local ingestion) is the next implementation milestone.

This is a serious infrastructure project, built in public, intended as both a working system and a portfolio artifact. Contributions follow the workflow in [AGENTS.md](AGENTS.md) and [AI_AGENT_WORKFLOW.md](docs/steering/AI_AGENT_WORKFLOW.md).

---

## Steering Documents

| Document | Purpose |
|----------|---------|
| [AGENTS.md](AGENTS.md) | Contract for AI agents and contributors |
| [PROJECT_CONTEXT.md](docs/steering/PROJECT_CONTEXT.md) | What Muninn is and why |
| [ARCHITECTURE_PRINCIPLES.md](docs/steering/ARCHITECTURE_PRINCIPLES.md) | Load-bearing principles |
| [LOCAL_FIRST_CONSTRAINTS.md](docs/steering/LOCAL_FIRST_CONSTRAINTS.md) | Hard constraints for local development |
| [DOMAIN_MODEL.md](docs/steering/DOMAIN_MODEL.md) | Core domain vocabulary |
| [EVENT_SCHEMA_STRATEGY.md](docs/steering/EVENT_SCHEMA_STRATEGY.md) | JSON now, Avro path |
| [DETERMINISTIC_REPLAY.md](docs/steering/DETERMINISTIC_REPLAY.md) | The most important doc |
| [SERVICE_BOUNDARIES.md](docs/steering/SERVICE_BOUNDARIES.md) | Module map |
| [TECH_STACK.md](docs/steering/TECH_STACK.md) | Every dependency, justified |
| [TESTING_STRATEGY.md](docs/steering/TESTING_STRATEGY.md) | Seven test layers |
| [OBSERVABILITY_STRATEGY.md](docs/steering/OBSERVABILITY_STRATEGY.md) | Logs, metrics, traces |
| [DATA_STORAGE_STRATEGY.md](docs/steering/DATA_STORAGE_STRATEGY.md) | Where data lives, and why |
| [ROADMAP.md](docs/steering/ROADMAP.md) | Phased plan |
| [AI_AGENT_WORKFLOW.md](docs/steering/AI_AGENT_WORKFLOW.md) | The agent loop |
| [CODING_STANDARDS.md](docs/steering/CODING_STANDARDS.md) | What to type |
| [NON_GOALS.md](docs/steering/NON_GOALS.md) | What we won't build |

---

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for attribution.

## See Also

- [CONTRIBUTING.md](CONTRIBUTING.md) — how to contribute as a human.
- [SECURITY.md](SECURITY.md) — how to report security issues.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — behavioral standards.
- [docs/steering/READING_GUIDE.md](docs/steering/READING_GUIDE.md) — which docs to read for your role.
- [docs/steering/GLOSSARY.md](docs/steering/GLOSSARY.md) — A–Z lookup of terms.
- [docs/adr/](docs/adr/) — Architecture Decision Records.
