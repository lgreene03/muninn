# AGENTS.md — Working on Muninn as an AI Agent

This document is the contract between AI coding agents (Claude Code, Antigravity, Cursor, etc.) and the Muninn codebase. Read it before making any change.

## Project Purpose

Muninn is an **event-native research infrastructure platform** for deterministic replay, reproducible streaming analytics, and market-data feature computation. It is local-first, infrastructure-focused, and explicitly **not** a trading bot.

Before doing anything else, read in order:

1. [docs/steering/PROJECT_CONTEXT.md](docs/steering/PROJECT_CONTEXT.md)
2. [docs/steering/ARCHITECTURE_PRINCIPLES.md](docs/steering/ARCHITECTURE_PRINCIPLES.md)
3. [docs/steering/NON_GOALS.md](docs/steering/NON_GOALS.md)
4. [docs/steering/DETERMINISTIC_REPLAY.md](docs/steering/DETERMINISTIC_REPLAY.md)

If your change conflicts with any of these, stop and surface the conflict — do not silently override.

## Architectural Constraints

- **Event-native.** Events are immutable, append-only facts. They are the only source of truth.
- **Deterministic by default.** Any computation over an event stream must produce identical results when replayed.
- **One computation path.** Live and replay paths share the same feature-computation logic. No "live-only" or "batch-only" branches.
- **Local-first.** The entire system must run on a Mac mini M4 with 24 GB RAM under Docker Compose. No managed cloud services in the MVP path.
- **Simple before distributed.** Choose DuckDB before Trino, Redpanda single-node before Kafka cluster, single-process Spring Boot before microservice mesh.
- **Observable by default.** Every service emits structured logs, metrics, and traces from day one.

## Coding Rules

- **Java 21**, Spring Boot, immutable `record` types for all domain models.
- **Explicit time types.** Use `Instant` for absolute time, `Duration` for elapsed time. Never `long` for time without a unit suffix in the variable name.
- **No hidden global state.** No static mutable fields. No singletons outside Spring's container.
- **Structured logging only.** SLF4J + key/value pairs. No string concatenation for log context.
- **Errors are typed.** Throw domain-specific exceptions, not bare `RuntimeException`.
- **Configuration via profiles.** `local-lite`, `local-full`, `cloud-cheap`, `production-reference`.
- **No dead code, no TODOs without an issue number.** Delete it or track it.

See [docs/steering/CODING_STANDARDS.md](docs/steering/CODING_STANDARDS.md) for the full set.

## Testing Expectations

Every change MUST be accompanied by tests appropriate to its layer:

- **Unit tests** for pure logic (feature computers, parsers, validators).
- **Contract tests** for any event schema change.
- **Deterministic replay tests** for any change to feature computation or stream processing — replay the same input twice, assert byte-identical output.
- **Testcontainers integration tests** for any change touching Kafka/Redpanda, PostgreSQL, or MinIO wiring.
- **Golden dataset tests** for feature engine changes.

If you cannot test the change, say so explicitly in your summary. Never claim success without verification.

See [docs/steering/TESTING_STRATEGY.md](docs/steering/TESTING_STRATEGY.md).

## Documentation Expectations

- Update the relevant steering doc **in the same commit** as any architectural change. Stale docs are worse than no docs.
- Add a one-line entry to [docs/steering/ROADMAP.md](docs/steering/ROADMAP.md) when completing a phase milestone.
- New modules require an entry in [docs/steering/SERVICE_BOUNDARIES.md](docs/steering/SERVICE_BOUNDARIES.md) **before** the first line of code is written.
- Every public package must have a `package-info.java` summarizing its responsibility.

## How to Make Changes Safely

1. **Read** the relevant steering docs.
2. **Plan** in writing before coding. State the change, the affected modules, the test approach, and any doc updates.
3. **Small commits.** One logical change per commit. Commit messages explain the *why*, not the *what*.
4. **Run the full test suite** before declaring done.
5. **Run the local smoke test** (`./scripts/smoke.sh`) if the change touches ingestion, replay, or storage.
6. **Summarize** changes in the PR description: what changed, why, what tests cover it, what's left.

## How to Propose New Modules

1. Open `docs/steering/SERVICE_BOUNDARIES.md` and draft an entry: responsibility, inputs, outputs, dependencies, non-responsibilities.
2. Confirm the module is necessary — can existing code be extended instead?
3. Confirm the module respects local-first constraints (memory budget, no managed services).
4. Only then create the package and the first commit.

## What NOT to Build

See [docs/steering/NON_GOALS.md](docs/steering/NON_GOALS.md) for the full list. The short version:

- No trading logic. No order routing. No execution.
- No price prediction models shipped as product features.
- No Kubernetes, Helm, or cloud-native infrastructure until Phase 8.
- No multi-exchange ingestion in the MVP path — one exchange, one or two symbols.
- No streaming engines (Flink/Spark) until the local stack is proven inadequate.
- No premature abstraction. No interfaces with one implementation "for future flexibility".

## Local-First Requirements

Any change that breaks `docker-compose up -d && ./scripts/smoke.sh` on a Mac mini M4 with 24 GB RAM is a **regression** and must be reverted or fixed before merge. See [docs/steering/LOCAL_FIRST_CONSTRAINTS.md](docs/steering/LOCAL_FIRST_CONSTRAINTS.md).

## Agent Workflow

See [docs/steering/AI_AGENT_WORKFLOW.md](docs/steering/AI_AGENT_WORKFLOW.md) for the prescribed loop: read → plan → test → code → doc → summarize.
