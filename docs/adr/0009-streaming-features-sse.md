# 0009. Server-Sent Events endpoint for the live feature stream

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Project maintainer
- **Related:** [ADR-0004 — Sealed event hierarchy](0004-sealed-event-hierarchy.md), [ADR-0006 — Trino query backend](0006-trino-query-backend.md), [SERVICE_BOUNDARIES.md](../steering/SERVICE_BOUNDARIES.md), [sleipnir/docs/TRIGGERS.md — T3](https://github.com/lgreene03/sleipnir/blob/main/docs/TRIGGERS.md)

## Context

The Query API (`GET /api/v1/features/{featureName}`, ADR-0006) answers historical, bounded-range questions by reading persisted Parquet/Iceberg through DuckDB or Trino. It is a pull, point-in-time interface: a client that wants to watch a feature evolve has to poll on a timer, trading latency against request volume, and always reading the warehouse for data that was already in flight on Kafka moments earlier.

The feature engine already publishes every `FeatureComputedEvent` to a per-feature Kafka topic (`features.<name>.<version>`, e.g. `features.vwap.1m.v1`) the instant a window closes — the warehouse is a downstream archival sink (ADR-0007), not the live path. A live consumer (a dashboard tile, a `muninn-py` notebook tailing a feature, huginn replacing its feature poll) wants the event when it is produced, not after it lands in object storage.

This is the trigger **T3** ("muninn ships a streaming features endpoint") in the shared cross-repo [trigger catalog](https://github.com/lgreene03/sleipnir/blob/main/docs/TRIGGERS.md). Shipping it promotes the WS/SSE-client items sitting in Phase F of muninn-py, huginn, and sleipnir.

Two transport choices were considered:

1. **Server-Sent Events (SSE).** Unidirectional server→client text stream over plain HTTP/1.1. Native browser support (`EventSource`), trivial `curl`-ability, and — decisively — it is served by Spring MVC's `SseEmitter` with no new runtime. The app is a servlet stack (`spring-boot-starter-web`); SSE fits without pulling in a reactive engine.
2. **WebSocket.** Bidirectional. Richer, but the stream is inherently one-way (the server pushes features; the client never talks back), and a first-class WebSocket story on a servlet stack means either `spring-websocket` plumbing or a WebFlux migration — cost with no matching benefit for a push-only feed.

## Decision

Ship **SSE** via Spring MVC `SseEmitter`. A new `io.muninn.streaming` package adds one endpoint and a single process-wide broadcast hub fed by one Kafka tail consumer.

`GET /api/v1/features/stream` (produces `text/event-stream`) opens a stream. Each `FeatureComputedEvent` is delivered as an SSE event named `feature` whose `data` is the JSON event and whose `id` is the event's UUID. An optional `?feature=<name>` query parameter restricts a connection to a single feature name; absent, the client receives every feature.

### One consumer, in-memory fan-out

A single `FeatureStreamConsumer` runs on one daemon thread and subscribes to the topic **pattern** `features\..*`, so new feature topics are picked up automatically with no code change. It uses a **unique consumer group per process** (`muninn-streaming-<uuid>`), starts from the **latest** offset, and **never commits** — the endpoint is a live tail, not a replayable cursor. It does not replay history; a client connecting at T sees events produced after T.

Every event it reads is handed to `FeatureStreamBroker`, which holds the set of connected `SseEmitter`s and fans the event out in memory to those whose optional filter matches. Kafka load is therefore **constant in the number of connected clients** — 200 dashboards do not create 200 consumer groups.

The endpoint surface and the broker always exist (so the HTTP route is always live); only the background tail thread is gated on `muninn.streaming.enabled`. When disabled, connections still open and receive keepalives, but no events flow.

### Connection lifecycle

- The broker registers `onCompletion`/`onTimeout`/`onError` callbacks that prune the subscriber and decrement the active-subscribers gauge. Pruning is idempotent (the active count only moves when the subscriber was actually present), so the completion and error callbacks can both fire without double-counting.
- A scheduled keepalive sends an SSE comment frame to every open connection on a fixed cadence (`muninn.streaming.keepalive-interval`, default 15 s) so idle reverse proxies hold the socket open and dead clients are detected and dropped between feature events.
- The server-side emitter timeout defaults to none (`muninn.streaming.emitter-timeout: PT0S`); liveness is the keepalive's job.

### Configuration shape

```yaml
muninn:
  streaming:
    enabled: true            # false → endpoint stays up (keepalives only), no events broadcast
    poll-timeout: PT0.5S      # Kafka poll() timeout for the tail loop
    emitter-timeout: PT0S     # PT0S → no server-side SSE timeout; rely on keepalives
    keepalive-interval: PT15S # comment-frame cadence to keep proxies open / prune dead clients
    # topic-pattern defaults to "features\..*"
```

### Metrics

Four Micrometer meters under the established `muninn.*` convention, all tagged `endpoint=features`:

- `muninn.streaming.subscriptions.active` (gauge) — connected clients.
- `muninn.streaming.events.received` (counter) — events read from Kafka by the tail consumer.
- `muninn.streaming.messages.sent` (counter) — events successfully pushed to a client (fan-out count).
- `muninn.streaming.disconnects` (counter) — clients pruned on completion, timeout, or send failure.

## Consequences

**Easier.**

- A browser tile or a notebook can watch a feature live with `EventSource("/api/v1/features/stream?feature=vwap.1m")` — no polling loop, sub-second latency from window-close to client.
- New features are streamable for free: the pattern subscription means shipping a new `features.<name>.<version>` topic needs no streaming-side change.
- The fan-out hub keeps Kafka cost flat as dashboards multiply.

**Harder / cost.**

- SSE state is in-memory and per-instance: behind a load balancer each replica tails Kafka and serves its own connections (fine — a unique group per process means every replica sees every event). There is no shared subscriber registry, and no cross-instance delivery guarantee; a client gets exactly the events produced while it was connected to *that* instance. This is acceptable for a live feed and explicitly **not** a replacement for the replayable Query API.
- It is a live tail with no backfill. A consumer that needs "the last hour then live" must page the Query API for history and then attach to the stream — the SSE endpoint deliberately does not replay.
- One more long-lived thread (the tail consumer) and one scheduled keepalive thread per instance. Both are daemon threads, stopped on context shutdown.

**Operational.**

- `enabled` defaults to true. Disabling it leaves the route up (keepalive-only) so health checks and reverse-proxy config don't change when the feed is turned off.
- The endpoint is unauthenticated, consistent with the rest of the read API; multi-user auth remains the reverse-proxy concern tracked under trigger **T14**.

## Alternatives Considered

- **WebSocket.** Rejected: the feed is push-only, so bidirectionality buys nothing, and first-class WebSocket support on a servlet stack is disproportionate plumbing. SSE is the minimal transport that fits.
- **Polling the Query API faster.** Rejected: it pushes warehouse-read load linearly with clients and poll rate, and still trails the live Kafka data by the archival lag. The whole point is to read the live path.
- **One Kafka consumer group per connected client.** Rejected: O(clients) consumer groups and partition rebalances; the in-memory broadcast hub achieves the same per-client filtering at constant Kafka cost.
- **A shared/distributed subscriber registry (e.g. Redis fan-out) across instances.** Rejected as premature: a unique-group-per-process tail already delivers every event to every instance's clients. Cross-instance coordination would add infrastructure for a guarantee a live feed does not need. Revisit only if a real requirement appears.

## References

- `src/main/java/io/muninn/streaming/FeatureStreamController.java` — the `GET /api/v1/features/stream` endpoint.
- `src/main/java/io/muninn/streaming/FeatureStreamBroker.java` — in-memory fan-out hub, keepalive, metrics.
- `src/main/java/io/muninn/streaming/FeatureStreamConsumer.java` — the Kafka tail (pattern-subscribed, unique group, latest, no commit).
- `src/main/java/io/muninn/streaming/StreamingConfiguration.java` — bean wiring and the `SmartLifecycle` that runs the tail thread.
- `src/main/java/io/muninn/streaming/StreamingProperties.java` — `muninn.streaming.*` config.
- `src/main/java/io/muninn/shared/event/FeatureComputedEvent.java` — the streamed payload and its `topicName()`.
- `src/test/java/io/muninn/architecture/ArchitectureRulesTest.java::streaming_does_not_depend_on_feature_ingestion_or_query` — the enforced boundary.
