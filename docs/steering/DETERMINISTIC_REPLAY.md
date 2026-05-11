# DETERMINISTIC_REPLAY.md

This is the most important document in the repository. Read it twice.

## What Deterministic Replay Means

**Determinism** in Muninn means: given the same sequence of input events and the same code version, the feature engine produces the same sequence of output events — byte-for-byte identical — every time it is run, on any machine.

**Replay** means: re-running the feature engine over historical events from the event log, producing outputs that are identical to those that would have been produced (or were produced) when those events first arrived live.

**Deterministic replay** is the conjunction: a system in which replay is possible *and* yields outputs that are bit-identical to the live path.

## Why It Matters

A system without deterministic replay cannot answer the question: *"what would my system have done in this historical scenario?"* Every answer is conditioned on the unrecoverable particularities of the moment: a wall-clock read, a network jitter, a thread scheduling order, a random seed lost to entropy.

Without determinism, every backtest is a fiction. Every audit is an estimate. Every debugging session is archaeology.

With determinism, the system has a property that ordinary software does not: **its behavior is a pure function of its inputs and its code.** That property is the foundation of every other useful property — reproducibility, auditability, rigorous experimentation, safe refactoring.

## How Live and Replay Share One Path

The feature engine is a function:

```
process: (state, event) -> (state', outputs)
```

It has no side channels. It does not read the wall clock. It does not call external services. It does not consult a database. Its only inputs are `state` (its own prior output) and `event` (from a source).

The **source** is the only thing that differs between live and replay:

- **Live source**: a Kafka consumer subscribed to the live topics.
- **Replay source**: a reader over the historical event log (Kafka with seek-by-timestamp, or Parquet via DuckDB).

Both sources implement a common `EventSource` interface that emits events in **event-time order** (with watermarks). The engine cannot tell which it is reading from.

```
+----------------+         +-----------------+         +-----------+
|  Live broker   | --read->|                 |         |           |
|  (Redpanda)    |         |   EventSource   | --emit->|  Feature  |
+----------------+         |                 |         |  engine   |
                           +-----------------+         |           |
+----------------+         +-----------------+         |  (pure)   |
|  Parquet log   | --read->|                 |         |           |
|  (MinIO)       |         |   EventSource   | --emit->|           |
+----------------+         +-----------------+         +-----------+
```

If the engine ever needs to "know" whether it is live or replay, that is a design failure. Fix the design.

## Event Ordering Assumptions

Within a single `(source, instrument)` partition, events arrive in `eventTime` order. Across partitions, ordering is best-effort: the engine merges multiple partitions using a **k-way merge by event time**, gated by watermarks.

Concretely:

- The engine maintains a watermark per partition.
- The global watermark is the minimum of per-partition watermarks.
- Events are released to the processor only when their `eventTime ≤ partitionWatermark`.
- Windows close when `windowEnd < globalWatermark`.

## Late Events

A "late event" is an event whose `eventTime` is below the current watermark when it arrives.

Policy (configurable per stream):

1. **Drop** with a counter increment and a structured log line. Suitable for ephemeral signals.
2. **Side-output** to a `late-events` topic for offline analysis. Suitable when late data is rare but meaningful.
3. **Revise** — emit a correction event for any affected window. Suitable when downstream consumers can handle revisions.

The chosen policy must be **identical between live and replay**. Replay is configured from the original run's manifest, not from default values.

## Checkpoints

Replay from `t=0` is always correct but often slow. Checkpoints accelerate it.

A checkpoint is a serialized snapshot of feature-engine state at a known watermark:

- Taken every N minutes of event time (configurable).
- Stored in MinIO as a typed binary file (Java serialization with a stable record schema, or Avro for cross-version readability).
- Indexed in PostgreSQL by `(featureName, featureVersion, watermark)`.

Replay can resume from the **most recent checkpoint** ≤ the requested replay start time, then process forward from there. If the requested start is before any checkpoint, replay starts from the event-log origin.

**Critical invariant:** a checkpoint is only valid for the **exact code version** that produced it. Loading a checkpoint with a different `featureVersion` is forbidden; the replayer must start from the previous checkpoint with a matching version, or from `t=0`.

## Divergence Detection

Determinism is only true if it is tested. Muninn runs continuous divergence checks:

- **Shadow replay**: the live path produces outputs to `features.live`. A shadow replayer reads the same input events from the log and produces outputs to `features.replay`. A comparator subscribes to both and emits a `replay.divergence` metric for any mismatch.
- **Periodic backfill audit**: nightly, replay a recent time window and compare against the live output bit-for-bit. Any mismatch pages an operator (in production-reference) or surfaces in the dashboard (locally).

A divergence is **never** treated as expected noise. It is always investigated.

## Reproducible Outputs

Every `FeatureComputedEvent` carries:

- `featureVersion` — the code SHA of the engine.
- `inputEventIds` — the events consumed to produce this output.
- `windowStart`, `windowEnd` — the event-time window.
- `processingTime` — the wall-clock time of emission (for SLA tracking only).

Given these, any consumer can ask the replay engine to re-derive the value and compare.

## How to Test Determinism

Three layers of test, all required:

### 1. Pure-function determinism (unit)

Each feature is implemented as a pure function. Tests assert:

```java
assertEquals(feature.compute(state, eventSequence),
             feature.compute(state, eventSequence));
```

The result must be `equals`-equal across two runs in the same JVM.

### 2. Replay determinism (integration)

A test fixture produces a known sequence of events into a Testcontainers Redpanda. The feature engine runs once and outputs to topic A. The engine restarts and replays from `t=0`, outputting to topic B. Topics A and B must be byte-identical event-for-event.

### 3. Cross-version safety (contract)

When `featureVersion` changes, a test runs the old code and the new code over the same input and asserts that **outputs differ in expected ways** (or are identical, if the change was a refactor). The diff is recorded in the PR.

## Anti-Patterns

The following are forbidden in feature-engine code:

- `Instant.now()`, `System.currentTimeMillis()`, `LocalDateTime.now()`.
- `new Random()` without a seeded constructor whose seed is part of the feature definition.
- Calls to external services (HTTP, gRPC, database) inside `process`.
- Reading mutable static fields.
- Using thread identity, hash codes of unordered collections, or iteration order of `HashMap` for any logic decision.
- Floating-point arithmetic where `BigDecimal` precision matters (prices, sizes, money).

Each anti-pattern has a `forbidden-patterns` static-analysis rule that fails the build.

## Summary

Determinism is not a feature. It is a discipline. It is enforced by the architecture (one computation path), the code (pure functions, explicit time), the tests (divergence detection at three layers), and the team (review checklists, AI-agent constraints).

If you are not sure whether a change preserves determinism, it doesn't.
