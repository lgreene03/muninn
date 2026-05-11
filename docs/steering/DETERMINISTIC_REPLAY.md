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

## Worked Example: VWAP Over Six Trades

The abstract framing above can hide what determinism actually feels like. Here is a concrete walkthrough using the real `VwapComputer` and a one-minute tumbling window starting at `14:00:00Z`.

### Inputs

Six `TradeEvent`s arriving at the feature engine, **in event-time order within their partition**:

| # | eventTime           | price (USDT) | size (BTC) |
|---|---------------------|--------------|------------|
| 1 | 14:00:05.000Z       | 60,000       | 0.10       |
| 2 | 14:00:12.000Z       | 60,010       | 0.05       |
| 3 | 14:00:30.000Z       | 60,005       | 0.20       |
| 4 | 14:00:55.000Z       | 60,020       | 0.10       |
| 5 | 14:01:02.000Z       | 60,030       | 0.15       |
| 6 | 14:01:10.000Z       | 60,025       | 0.10       |

The watermark advances with each event's `eventTime` minus a 1-second allowed lateness.

### State Transitions (live path)

```
t=14:00:05  event 1 ingested
            window [14:00, 14:01) opens
            running Σ(p·s) = 60000·0.10 = 6000.00
            running Σs    = 0.10
            watermark = 14:00:04

t=14:00:12  event 2 ingested
            running Σ(p·s) = 6000.00 + 60010·0.05 = 6000.00 + 3000.50 = 9000.50
            running Σs    = 0.15
            watermark = 14:00:11

t=14:00:30  event 3 ingested
            running Σ(p·s) = 9000.50 + 60005·0.20 = 21001.50
            running Σs    = 0.35
            watermark = 14:00:29

t=14:00:55  event 4 ingested
            running Σ(p·s) = 21001.50 + 60020·0.10 = 27003.50
            running Σs    = 0.45
            watermark = 14:00:54

t=14:01:02  event 5 ingested (belongs to next window)
            watermark advances to 14:01:01
            watermark (14:01:01) >= windowEnd (14:01:00) → window [14:00, 14:01) closes
            VWAP_1 = 27003.50 / 0.45 = 60007.7777...
            FeatureComputedEvent emitted:
              featureName = "vwap"
              featureVersion = "vwap@<sha>"
              windowStart = 14:00:00Z
              windowEnd   = 14:01:00Z
              value       = 60007.78 (BigDecimal, HALF_UP, 2dp)
              inputEventIds = [id1, id2, id3, id4]
            new window [14:01, 14:02) opens with event 5:
              running Σ(p·s) = 60030·0.15 = 9004.50
              running Σs    = 0.15

t=14:01:10  event 6 ingested
            running Σ(p·s) = 9004.50 + 60025·0.10 = 15007.00
            running Σs    = 0.25
            watermark = 14:01:09
            (window [14:01, 14:02) does not close yet)
```

### Live Output

Exactly one `FeatureComputedEvent` so far:

```
windowStart  = 14:00:00Z
windowEnd    = 14:01:00Z
value        = 60007.78
inputEventIds = [id1, id2, id3, id4]
featureVersion = "vwap@<sha>"
```

### Replay Path

Now imagine we replay the same six events tomorrow against the same `featureVersion`.

The `ReplayEventSource` reads from Parquet (or from Kafka with seek-by-timestamp) and emits the events **in the same partition order** with **the same `eventTime`s**. The `FeatureEngineRunner` does not know it is a replay. The same `WindowManager` opens the same window. The same `VwapComputer` is invoked with the same `WindowedBatch`.

Because:

- The arithmetic is `BigDecimal` (no floating-point drift).
- The window assignment depends only on `eventTime` and the configured window size.
- The watermark depends only on the events seen and the configured lateness — no wall-clock involvement.
- The output `FeatureComputedEvent` populates fields purely from the inputs and the `featureVersion`.

…the replayed output is **byte-for-byte identical** to the live output. A divergence detector reading both outputs sees zero difference.

### What Would Break It

Five hypothetical changes that would break determinism, and how the system catches each:

| Change | What breaks | How it is caught |
|---|---|---|
| `VwapComputer` reads `Instant.now()` to timestamp its output | Replayed output has a different `processingTime`, and if the field were `eventTime` it would be catastrophic | ArchUnit rule forbids `Instant.now()` in `feature.compute.*`; the divergence detector would see processingTime differ |
| Window state stored in a `HashMap` iterated for output | Iteration order differs across JVMs; sums in a different order; with floats this would produce different bits (with BigDecimal in this case, it would still be deterministic, but the principle stands) | `VwapDeterminismTest` runs the same input twice and asserts equality |
| New `vwap@1.0.1` version silently activated while old checkpoints remain | Replay loads a checkpoint produced by `vwap@1.0.0`, applies new logic, output diverges | `CheckpointManager` rejects checkpoints whose `featureVersion` does not match the running engine |
| Late event (eventTime = 14:00:58, arriving after window closed) admitted without policy | Live processes it, replay does not (or vice versa) | Late-event policy is set per stream and recorded in the replay manifest; both paths apply identical policy |
| Floating-point sums in the inner loop | Bit-different totals across machines or even instructions | ArchUnit rule forbids `double` arithmetic in money-handling packages; `BigDecimal` is mandatory |

### What This Demonstrates

Determinism is not a property a single test confirms. It is what remains when every potential source of variability has been removed by **design** (sealed types, BigDecimal, explicit time, single computation path), **lint** (ArchUnit forbidden patterns), and **tests** (unit determinism + cross-JVM replay + shadow-replay divergence detection).

The example above is small. The discipline scales because the rules don't change with the size of the feature — they apply to every computation in the system.

## Summary

Determinism is not a feature. It is a discipline. It is enforced by the architecture (one computation path), the code (pure functions, explicit time), the tests (divergence detection at three layers), and the team (review checklists, AI-agent constraints).

If you are not sure whether a change preserves determinism, it doesn't.
