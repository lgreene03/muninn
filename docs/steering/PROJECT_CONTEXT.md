# PROJECT_CONTEXT.md

## What is Muninn?

Muninn is a **local-first, event-native research infrastructure platform**. It ingests immutable market-data events, persists them as an append-only log, and computes derived features (candles, order-book aggregates, statistical signals) in a way that can be **replayed deterministically** at any later time.

Named after one of Odin's two ravens — *Muninn* means "memory." The other raven, *Huginn* ("thought"), is reserved for a possible companion project focused on online inference. Muninn is the memory: durable, immutable, replayable.

## What Problem Does It Solve?

Quantitative research and streaming-analytics work has a chronic correctness problem: **the system used to develop a feature is rarely the same system that runs it in production.** Research notebooks operate on cleaned historical CSVs. Production operates on a live stream. The two diverge silently. Backtests pass; live deployments fail; no one can tell why.

Muninn solves this by making **one event log the source of truth** and **one computation path serve both live and historical workloads**. The same feature engine that computes a value from yesterday's events computes it from the live stream — bit-for-bit identical.

Concretely:

- A researcher writes a feature definition once.
- The feature engine runs it live as events arrive.
- The same feature engine replays it over the historical event log, producing identical outputs.
- Divergence between live and replay is a detectable bug, not an accepted reality.

## Why Replayability Matters

A system that cannot replay its history cannot:

- **Debug.** Reproducing a production incident requires reproducing the input.
- **Audit.** Regulators, risk teams, and clients ask: "what did the system see, and what did it decide?"
- **Improve.** Backtesting a new feature against old data is meaningless if the old data was processed under different code.
- **Onboard.** New engineers cannot reason about a system whose state depends on unrecoverable history.

Replay is not a feature. It is a property of the system's design. Muninn treats it as a first-class invariant.

## Why Live/Replay Parity Matters

If "live" and "replay" run different code, they will eventually disagree. Then every alert becomes ambiguous: is this a real signal, or a divergence?

Muninn enforces parity at the architectural level: there is no separate "batch" engine. The feature engine is a deterministic function `(state, event) → (state', output)`. Live and replay differ only in their event source.

## Target Users

- **Quantitative researchers** who want their notebook code to behave identically in production.
- **Streaming-analytics engineers** building reproducible feature pipelines for non-financial domains (telemetry, IoT, observability).
- **Infrastructure engineers** who need a reference implementation of event-sourced, deterministic stream processing on commodity hardware.
- **Educators and learners** who want a real, working example of event-native architecture without a cloud bill.

## Target Hiring Signal

Muninn is also a serious portfolio artifact. It demonstrates:

- Event-sourcing and stream-processing architecture in practice.
- Disciplined separation of immutable inputs from derived outputs.
- Production-shaped operational concerns (observability, schema evolution, replay) on a local-first footprint.
- Restraint: a small surface area, well-tested, well-documented, with explicit non-goals.

The goal is to show systems-level judgment, not feature volume.

## Why This Is Infrastructure, Not a Trading Bot

Muninn does not place orders. It does not predict prices. It does not optimize portfolios. It does not give financial advice.

It is a **substrate** on which research and analytics can be done reproducibly. Crypto exchange APIs are used as the initial data source because they are free and public — not because Muninn is a crypto project. The same architecture serves IoT sensor streams, server telemetry, or any other event-native domain.

If you came here looking for alpha, you are in the wrong repository. If you came here looking for an honest, well-built piece of streaming infrastructure, welcome.
