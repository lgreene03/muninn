# What deterministic replay actually means

*Draft, 2026-05-18. AI-assisted draft, reviewed by the project maintainer. Audience: engineers familiar with streaming systems who already think "replay" sounds obvious.*

The first thing people say when they read Muninn's README is: "replay isn't new." That's true. The second thing they say is: "you're talking about backfill — every system has that." That's also true, in a sense that's harmless. The third thing — and this is where the gap usually appears — is: "so the live and historical paths give similar answers?"

Similar isn't the claim. Identical is.

Specifically: the computed value the system emits when an event arrives live is the same `BigDecimal` the system emits when you replay that event a year later. Same window boundary. Same set of input event IDs cited as provenance. Same code version on the output. Down to the byte.

That's the property Muninn enforces, and the rest of this post is about *how* — which is more interesting than the claim itself, because the work isn't in writing the live path or the replay path. It's in making sure they're the same path.

## The shape of the problem

Most streaming-analytics systems I've worked on had a "live" pipeline and a "historical" pipeline that grew up at different times for different reasons. Maybe the live one is Kafka Streams or Flink; maybe the historical one is Spark batch over Parquet. Each one is sensible. Their outputs disagree all the time, and nobody is surprised, because:

- The live path reads from a streaming source with watermarks and late-event policy.
- The historical path reads from a sorted dataset with no watermark concept.
- They use different SQL dialects. Different time-handling. Different null-vs-zero conventions.
- The live path has wall-clock-based logic ("if we haven't seen an update in 30 seconds, emit"). The historical path doesn't.
- Their numeric types differ — one uses `double`, the other `BigDecimal`. Or one rounds at output time, the other doesn't.
- The live path's lateness allowance is 30 seconds; the historical path doesn't have one because it knows it's seen everything.

Every one of those is a place where the two outputs drift. The drifts compound. By the time anyone notices, you can no longer reproduce a production decision from historical inputs, and your backtests are fiction.

The conventional fix is "make them more similar". Spend an engineering year on a unified job model — Flink for both. Move both to `BigDecimal`. Standardize lateness handling. This works in proportion to the effort you put in. It doesn't ever quite finish, because the two paths are conceptually different programs that happen to be doing similar work.

Muninn's response is structural: don't have two paths. One feature engine, two sources. The engine doesn't know which source it's reading from. If it ever needed to, that's a design failure, fix the design.

## What the code actually looks like

There's a `FeatureEngineRunner` class. It implements `Runnable`. Its loop is roughly:

```java
while (running && eventSource.hasMore()) {
    Optional<EventSource.PartitionedEvent> polled = eventSource.poll();
    if (polled.isEmpty()) continue;

    EventSource.PartitionedEvent pe = polled.get();
    MarketEvent event = pe.event();

    if (event instanceof TradeEvent trade) {
        windowManager.add(trade, pe.partition());
        // ... checkpoint bookkeeping, watermark updates ...
    }

    windowManager.fireCompletedWindows(batch -> {
        FeatureComputedEvent result = VwapComputer.compute(batch, config.codeVersion());
        featureProducer.send(topicResolver.apply(result.topicName()), key, result);
    });
}
```

The `EventSource` is an interface with two implementations:

- `LiveEventSource` wraps a Kafka consumer subscribed to `events.trade`.
- `ReplayEventSource` wraps a Kafka consumer that's been seeked to a historical timestamp.

The runner doesn't care which. It receives `PartitionedEvent`s in event-time order. It maintains watermarks per partition. When a window closes (`globalWatermark > windowEnd`), it fires the batch through `VwapComputer.compute(...)`, which is a pure function.

The only thing that varies between live and replay is one constructor parameter — `topicResolver` — which is `identity` for live and `t -> t + ".replay"` for replay. That's not part of the computation; it's where the output lands. Live writes to `features.vwap.1m.v1`; replay writes to `features.vwap.1m.v1.replay`. Same record contents in either case.

## What "pure function" buys you

`VwapComputer.compute(WindowedBatch batch, String codeVersion)` is the actual computation. It takes a sorted-by-event-time list of `TradeEvent`s, sums `price * size` and `size` separately, divides, returns a `FeatureComputedEvent` with the resulting `BigDecimal`. There is nothing else in it. No clock read. No random. No external call. No mutable static state.

That's enforced. ArchUnit, at build time, refuses to merge code that:

- Calls `Instant.now()`, `System.currentTimeMillis()`, or `System.nanoTime()` inside any package matching `io.muninn.feature.compute..`.
- Constructs `new Random()` anywhere in `io.muninn.feature..`.
- Annotates fields with `@Autowired` (we're constructor-injection-only; a field-injected static is a back door to non-deterministic state).
- Calls `Throwable.printStackTrace()` (a small thing, but it leaks ordering through stderr in ways that have bitten me before).

Every one of those rules is in `ArchitectureRulesTest.java`. They run on every CI build. They catch the things humans forget at 2 AM.

`BigDecimal` is enforced by convention rather than ArchUnit (the rule "no `double` arithmetic in money-handling packages" is on the deferred list because the allowlist is fiddly). But every numeric column in the canonical event types is typed `BigDecimal`, and the determinism integration test (more in a second) would catch a floating-point regression immediately.

## How we test it

There's a test called `ReplayDeterminismIntegrationTest`. It does this:

1. Spins up Testcontainers — real Kafka, real Postgres.
2. Produces six `TradeEvent`s into `events.trade` with known event-times spanning two minutes.
3. The live feature engine consumes them, computes two windows of VWAP, writes to `features.vwap.1m.v1`.
4. The test submits a replay job via `POST /api/v1/replay/jobs` for the same event-time range.
5. The replay engine — a fresh `FeatureEngineRunner` instance with a `ReplayEventSource` — consumes the same events from the same broker (different consumer group), computes the same windows, writes to `features.vwap.1m.v1.replay`.
6. The test consumes from both topics, sorts by `windowStart`, and asserts every output pair is equal on `windowStart`, `windowEnd`, `featureName`, `featureVersion`, `codeVersion`, `value` (via `BigDecimal.compareTo`), and `inputEventIds.size()`.

If that test ever turns red, something has broken determinism. It runs on every push to `main`. The green CI badge on the README is enforcing this claim continuously, not advertising it.

There's a continuous version too. The `ShadowReplayComparator` is a Spring `@KafkaListener` that subscribes to both the live topic and any `.replay` sibling. It pairs up events by `(featureName, featureVersion, windowStart)` and runs the same comparison. Every divergence increments a Prometheus counter; a default alert rule fires if it exceeds zero in any 5-minute window. The plan is to wire a nightly shadow-replay job in production-reference so this is checked against rolling 24-hour ranges; that's queued, not built yet.

## The honest scope of the claim

There is one place where live and replay outputs differ, and we documented it in ADR-0002.

The `FeatureComputedEvent` carries an `eventId`. We generate it as UUIDv7 via `UUIDv7.generate()`, which reads `System.currentTimeMillis()`. So every replay generates a different `eventId` from the live run, even though every other field matches.

That's fine, and the integration test explicitly excludes `eventId` from its assertions, but it does mean the literal claim "byte-identical" needs one footnote. The ADR explains:

> The feature engine's determinism claim covers `featureName`, `featureVersion`, `codeVersion`, `windowStart`, `windowEnd`, `value`/`values`, and the set of `inputEventIds`. The `eventId` of a `FeatureComputedEvent` is allowed to differ across runs. It is provenance metadata — a per-emission identifier used for tracing and observability — not part of the computational claim.

If a future requirement demanded strict byte-identity (say, an auditor's reproduction harness), the fix is documented in the same ADR: derive `eventId` from a hash of the determinism-relevant fields. We didn't do it because no current consumer needs that, and the alternative — hashing — introduces its own versioning surface.

## What this isn't

Worth being precise about. Muninn's deterministic replay doesn't:

- **Eliminate non-determinism between code versions.** A bug fix in `VwapComputer` changes its `codeVersion`. Old outputs and new outputs live on different topics and aren't comparable. That's correct — they shouldn't be. The system makes that distinction first-class rather than hiding it.
- **Replay external IO.** The system doesn't call external services from inside computation, so there's nothing to mock. If a future feature needs to enrich events with reference data, that lookup is pre-materialized — by writing the reference value into the event before it enters the feature engine.
- **Handle non-deterministic upstream sources gracefully.** If two exchanges' WebSocket feeds disagree on which trade happened first, the system uses the source-reported `sequenceNumber` and `eventTime`. We assume the source is the canonical version. If the source is wrong, we're wrong. That's a property of the world, not of Muninn.
- **Eliminate the need to backfill.** When you add a new feature, you have to compute it over historical data once. The replay engine is exactly the tool for that — submit a job, wait, the new feature's outputs appear in their version-tagged topic.

## Why this is worth doing

If you've operated a streaming system in production, you've probably had one of these conversations:

> "The signal looked good in backtest. Why is the live system flat?"
> "Are we using the same data?"
> "The columns are named the same, but..."
> *(several days)*
> "Oh, the live one rounds to two decimals before averaging and the historical one doesn't."

The cost of that conversation isn't the bug. It's the loss of trust in everything else. Once a team has been burned twice, every backtest result is suspect. Every alert is conditioned on "is this real or is it the divergence again?". The system stops being a source of truth and becomes a source of friction.

Determinism eliminates that conversation. Not by making both pipelines more careful — that's the never-finishing approach. By making them the same pipeline.

The cost is up front: it's harder to introduce a new feature, because the feature engine's pure-function contract means you can't shortcut to a wall-clock-aware optimization. The benefit is permanent: any number the system produces, you can re-derive from inputs at any later time, and check that it still matches.

## If you want to play with it

Muninn is at <https://github.com/lgreene03/muninn>. There's a [DEMO.md](https://github.com/lgreene03/muninn/blob/main/docs/DEMO.md) that walks through booting the stack, sending trades, running a replay, and verifying zero divergence. Takes about ten minutes.

The Python SDK at <https://github.com/lgreene03/muninn-py> pulls features into Polars DataFrames for notebook work. Same code paths, same determinism guarantees. There's a `notebooks/alpha_backtest_demo.ipynb` that demonstrates pulling features, computing forward returns, and submitting a replay from a notebook.

Both are Apache 2.0. Neither is for sale. The project is built in public as infrastructure that does one thing well, in case it's useful to someone trying to solve the same problem.
