# 0015. Generalize the feature engine to dispatch OBI, micro-price and VPIN alongside VWAP

- **Status:** Accepted
- **Date:** 2026-09-15
- **Deciders:** feature-engine maintainers
- **Related:** [norse-stack/docs/ROADMAP.md](https://github.com/lgreene03/norse-stack) Phase P2 ("One feature-computation path"); [ADR-0002](0002-event-id-determinism.md); [ADR-0011](0011-sealed-event-hierarchy.md)

## Context

muninn already ingested Binance `depth20@100ms` book snapshots onto `events.book.snapshot`
and already had mathematically correct `OrderBookImbalanceComputer`, `MicroPriceComputer`
and `VPINComputer` classes (Phase 9). None of them were dispatched: `FeatureEngineRunner`
only admitted `TradeEvent`s, `WindowManager` was typed to `TradeEvent`, and the run loop
called `VwapComputer.compute(batch, codeVersion)` directly rather than iterating a
registry. Two independent, cross-repo reimplementations of "the same" features
(`obi-bridge` in norse-stack, `cmd/fetcher/aggregate.go` in huginn) filled the gap this
left in the live signal path — the subject of the wider norse-stack Phase P2 investigation.
This ADR covers muninn's half: making the engine genuinely own feature computation so
those duplicates have something to be retired in favour of.

Wiring VPIN in particular forced a real design question. VPIN's original implementation
held its volume-bucket accumulation as mutable instance fields — directly violating
`FeatureComputer`'s own pure-function contract, and, because the fields were not keyed by
instrument, unsafe the moment more than one instrument shares an engine. But VPIN's volume
buckets do not align to the engine's time windows: a bucket can span several windows, or
several buckets can fill within one window. A computer that is a genuine pure function of
"this window's batch alone" (as VWAP, OBI and micro-price all are) cannot express that.

## Decision

`WindowedBatch` and `WindowManager` are generalized from `TradeEvent` to `MarketEvent`, so
a single tumbling window can hold trades and book snapshots together; each computer
filters the batch down to the event type it needs (`WindowedBatch.trades()` for trade-only
computers). `FeatureEngineRunner` dispatches over a `List<WindowFeatureComputer>` —
VWAP, OBI and micro-price all fit that one-call-per-window shape. VPIN does not, and is
therefore handled separately: it is rewritten as a static function
`VPINComputer.compute(BucketState state, WindowedBatch batch, ...)` that takes its
volume-bucket state explicitly and returns the next state, with no mutable field on the
class itself. `FeatureEngineRunner` owns that state in a `Map<Instrument, BucketState>`,
mirroring how it already owns `WindowManager`'s buffered-window state — the computer stays
pure; the orchestrator carries state across calls, exactly as `DETERMINISTIC_REPLAY.md`
prescribes for the rest of the engine.

## Consequences

- Book snapshots reaching the live engine required subscribing `LiveEventSource` to
  `events.book.snapshot` as well as `events.trade`, and fixing `ReplayJobRunner`'s consumer
  properties (it hardcoded a `TradeEvent` JSON default type that would have silently broken
  a replay job over snapshot topics had the type header ever been absent).
- `OrderBookSnapshotEvent` (and `PriceLevel`) needed to become `Serializable`: once a
  buffered window can hold a snapshot, `CheckpointManager`'s Java-serialization checkpoint
  write would otherwise silently fail for that window. This was caught by a test, not
  inferred — see `FeatureEngineRunnerDispatchTest`.
- The engine-level checkpoint's identifying key could no longer be `VwapComputer.FEATURE_NAME`
  — the buffered window state it captures is shared across every registered feature now,
  not VWAP's alone. It is renamed to `FeatureEngineRunner.ENGINE_CHECKPOINT_NAME`.
- VPIN's bucket state is *not* included in that checkpoint. A restart resets VPIN's moving
  average to a cold start. This is a known, scoped-out gap, not an oversight — extending
  `CheckpointState` to carry per-instrument VPIN state is follow-up work, not part of this
  change.
- `muninn.feature.events.processed` is now tagged by the ingested event's own topic rather
  than a fixed feature name (an event can now feed more than one feature), and
  `muninn.feature.outputs.emitted` / `muninn.feature.latency` are tagged dynamically per
  emitted event rather than fixed to VWAP at construction time. `OBSERVABILITY_STRATEGY.md`
  is updated accordingly.
- `ShadowReplayComparator` subscribes via `topicPattern` (matching the
  `"features." + name + "." + version` convention `FeatureComputedEvent#topicName()` already
  enforces) instead of a hardcoded VWAP topic pair, so a fifth registered feature needs no
  change here.
- The `FeatureComputer` interface (`compute(FeatureDefinition, Iterable<MarketEvent>) ->
  Map<String,Object>`) is left as-is for `SignalEvaluator`'s incremental, per-event
  backtest-harness use, but is no longer how the live engine dispatches — OBI and
  micro-price were rewritten to the same static, `WindowedBatch`-based shape as
  `VwapComputer` instead, since that shape is what actually needed provenance
  (`windowStart`/`windowEnd`/`codeVersion`/`inputEventIds`) to produce a
  `FeatureComputedEvent` directly.

## Alternatives Considered

- **Force VPIN into the same stateless `WindowFeatureComputer` shape as the others**, by
  recomputing its entire bucket history from event-log origin on every window. Rejected:
  unbounded recomputation cost that grows with the run's age, for a state shape
  (`WindowManager` checkpointing) the codebase already has a working precedent for solving
  the right way.
- **Route dispatch through `feature_definitions` in Postgres at runtime**, making
  `ShadowReplayComparator` and the runner read the DB table live. Rejected: nothing else in
  the engine does this today — `vwap.1m`'s own window duration, feature name and version are
  code constants, and the DB row is discovery metadata for `GET /api/v1/features` only (see
  `FeatureCatalogService`). Introducing a first DB-driven runtime dependency just for this
  change, when a code-level topic-naming convention (`topicPattern`) solves the actual
  problem (a hardcoded VWAP-only topic pair) with less new machinery, was the more
  proportionate fix. A DB-backed, dynamically-registered feature catalog is a larger,
  separate piece of future work (see `ARCHITECTURE_REVIEW.md` "No feature registry").

## References

- `docs/steering/DETERMINISTIC_REPLAY.md` §How Live and Replay Share One Path, §Checkpoints
- `VPINComputer`, `WindowFeatureComputer`, `FeatureEngineRunner` Javadoc
