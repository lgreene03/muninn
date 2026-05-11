# ARCHITECTURE_PRINCIPLES.md

These principles are load-bearing. They override convenience, fashion, and the desire to ship faster. Violating one requires an explicit, written justification in the relevant steering doc.

## 1. Event-Native

Events are the system's primary data structure. Every meaningful state change is recorded as an immutable event before any derived view, cache, or aggregate is computed. There is no privileged "current state" table; current state is a fold over the event log.

**Implication:** the event log is the canonical persistence boundary. Lose it, and the system is lost. Keep it intact, and all derived state can be rebuilt.

## 2. Deterministic by Default

Every computation over the event stream must be a pure function of its inputs and prior state. Given the same inputs in the same order, it must produce identical outputs — byte-for-byte. Non-determinism (wall-clock reads, random seeds, network IO inside computation) is a defect.

**Implication:** time is an input to the computation, not an ambient property. Random seeds are explicit and recorded. External lookups are pre-materialized.

## 3. Immutable Inputs

Events are append-only. They are never updated, deleted, or rewritten. Corrections are themselves events (`PriceCorrected`, `TradeRetracted`) that the consumer applies in order.

**Implication:** the system's truth is monotonically growing. A bug never "loses" data; the buggy code is replaced and the events are replayed.

## 4. Derived Outputs Are Reproducible

Every derived artifact — candles, features, snapshots, indices — must be reproducible from the event log alone. Derived state is **cache**, not truth. It can be deleted and rebuilt at any time.

**Implication:** never store a derived value without also storing the inputs and code version that produced it. Without that, reproduction is impossible.

## 5. One Computation Path for Live and Replay

There is **one** feature engine. It accepts events from a source. The source may be the live broker or the historical event log. The engine does not know the difference and must not behave differently.

**Implication:** no "live mode" flags inside computation code. No "if backfill then else." If the live path is faster, the replay path must catch up; if the replay path is more careful, the live path must be made equally careful.

## 6. Explicit Time Handling

Two distinct clocks exist:

- **Event time** — when the event happened in the world (from the source).
- **Processing time** — when the system observed or processed it.

Every computation declares which clock it uses. Windows are over event time by default. Watermarks gate late-event handling. Wall-clock reads are forbidden inside feature logic.

**Implication:** clock skew, network delay, and out-of-order arrival are first-class concerns, not edge cases to be patched later.

## 7. Local-First

The full system must run on a single Mac mini M4 with 24 GB RAM, using Docker Compose, with no managed cloud services. Any feature that cannot satisfy this constraint belongs in the production-reference phase, not the MVP.

**Implication:** prefer DuckDB over Trino, Redpanda single-node over Kafka cluster, MinIO over S3, embedded over distributed. Scale up only when local pain is real and measurable.

## 8. Observable by Default

Every service emits structured logs, Prometheus metrics, and OpenTelemetry traces from its first commit. There is no "we'll add observability later." Replay-divergence, ingest lag, feature latency, and broker lag are tracked as named metrics.

**Implication:** debugging is done from telemetry, not from log-fishing. New code that does not emit telemetry is incomplete.

## 9. Simple Before Distributed

Single process before microservices. Single broker before cluster. Single node before sharded. The bar to distribute is a measured local-stack bottleneck, not a hypothetical scalability concern.

**Implication:** YAGNI applies aggressively. Most production systems are over-engineered; Muninn aims to be right-engineered.

## 10. Production-Shaped, Not Production-Heavy

The system looks like a real production system in its boundaries, contracts, and operational concerns — but runs at a hobbyist footprint. Configuration profiles (`local-lite`, `local-full`, `cloud-cheap`, `production-reference`) make the same codebase scale up without rewriting.

**Implication:** every abstraction must work at both ends. If a queue can only run on Kafka, it does not belong in MVP. If a queue can run on Redpanda locally and Kafka in production with no code change, it does.
