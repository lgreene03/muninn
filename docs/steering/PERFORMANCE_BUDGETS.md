# PERFORMANCE_BUDGETS.md

Performance is a property, not a hope. This document defines named, testable budgets per service. They live here so that changes that violate them are visible in PR review, and so that CI can enforce them once the harness exists.

A budget is not a benchmark. It is a **ceiling** the system commits to staying under on the reference hardware. Going faster is welcome; going slower than budget is a regression that must be fixed or the budget renegotiated in a PR.

## Reference Hardware

All budgets are measured on:

- Mac mini M4, 24 GB unified memory.
- Docker Desktop or OrbStack with default resource allocation.
- No other heavy applications running.

`cloud-cheap` and `production-reference` budgets are documented separately under each section.

## Throughput Budgets

| Component | Local-full p95 throughput | Notes |
|---|---|---|
| Ingestion (single Binance feed) | ≥ 5,000 events/sec sustained | Trades + depth, normalized and validated |
| Feature engine (single VWAP feature) | ≥ 10,000 events/sec sustained | Single-threaded loop, per [FeatureEngineRunner](../../src/main/java/io/muninn/feature/engine/FeatureEngineRunner.java) |
| Parquet rollover writer | ≥ 50,000 events/sec batched | Single file per partition per minute |
| Query API (cached) | ≥ 200 req/sec | Single instance |
| Query API (DuckDB cold scan) | ≥ 20 req/sec | Hot Parquet partition |

Sustained means: ≥ 5 minutes without backpressure or GC pause exceeding 100 ms.

## Latency Budgets

| Path | p50 | p95 | p99 |
|---|---|---|---|
| Ingestion: WebSocket message → Kafka acknowledged | 10 ms | 30 ms | 100 ms |
| Feature engine: trade arrives → window output emitted (within window) | 100 ms | 500 ms | 2 s |
| End-to-end: exchange event-time → feature output ingestTime | < 1 s | < 3 s | < 10 s |
| Query API: feature time-series (1 hour window) | 50 ms | 200 ms | 500 ms |
| Query API: replay-job status | 10 ms | 30 ms | 100 ms |
| Health check (`/actuator/health`) | 5 ms | 20 ms | 100 ms |

End-to-end latency includes the natural delay of waiting for a window to close, which is bounded by the window size, not by Muninn. The budget assumes a 1-minute tumbling window with a 1-second allowed-lateness.

## Memory Budgets

Already specified in [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md). Repeated here for completeness, with per-component JVM heap caps:

| Service | Container cap | JVM `-Xmx` |
|---|---|---|
| ingestion-service | 1 GB | 768 MB |
| feature-engine | 2 GB | 1.5 GB |
| replay-engine | 2 GB | 1.5 GB |
| query-api | 1 GB | 768 MB |

The current MVP runs all of these inside a single Spring Boot process whose container cap is 4 GB and JVM heap is 3 GB. Splitting into separate processes (Phase 5+) inherits the per-service caps above.

## Startup Budgets

| Phase | Budget |
|---|---|
| Cold start (`docker-compose up -d` from scratch) | ≤ 5 min |
| Warm start (containers already pulled) | ≤ 90 s |
| Single Spring Boot process boot | ≤ 30 s |
| Smoke test (`./scripts/smoke.sh`) end-to-end | ≤ 90 s |

Cold-start budget is the contract from [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md).

## Storage Budgets

| Artifact | Daily growth (single BTC-USDT feed) | Notes |
|---|---|---|
| Raw trade events (Parquet) | ≤ 100 MB/day | Trades + book snapshots, compressed |
| Feature outputs (Parquet) | ≤ 10 MB/day | VWAP at 1-minute windows |
| PostgreSQL metadata | ≤ 1 MB/day | Replay cursors, job state |
| Redpanda hot retention (7 days) | ≤ 1 GB | All topics combined |

The 256 GB free-disk minimum in [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md) holds about 6 years of single-instrument data before any compaction. Multi-instrument scaling is a Phase 8 concern.

## Test-Suite Budgets

| Suite | Budget |
|---|---|
| Unit tests only (`mvn test`) | ≤ 60 s |
| Contract + unit | ≤ 90 s |
| Integration tests (Testcontainers) | ≤ 5 min |
| Determinism tests | ≤ 60 s |
| Full `mvn verify` | ≤ 8 min |

Tests slower than these are a defect, even when they pass. Slow tests erode the workflow.

## Coverage Budgets

| Module / package | Line coverage gate |
|---|---|
| `io.muninn.shared.validation.*` | 100% |
| `io.muninn.shared.event.*` | 100% |
| `io.muninn.feature.compute.*` | 100% |
| `io.muninn.feature.engine.*` | 90% |
| `io.muninn.ingestion.adapter.*` | 90% |
| Everything else | 80% |

These gates are enforced by JaCoCo in CI (configuration pending). Coverage below the gate fails the build.

## Replay Budgets

| Scenario | Budget |
|---|---|
| Replay 1 hour of historical events through one feature | ≤ 60 s |
| Replay 1 day of historical events through one feature | ≤ 20 min |
| Replay 7 days from cold cache | ≤ 4 hours |
| Divergence detection (live vs shadow, 1-hour audit) | ≤ 90 s |

Replay should outpace live by at least 50× for short ranges. Below 10× and replay becomes operationally painful.

## How These Budgets Are Verified

- **Unit-test perf assertions** for hot-path methods (parser throughput, window-firing latency).
- **JMH micro-benchmarks** in `src/jmh/` (planned) for the feature-engine inner loop.
- **Smoke-test timing** asserted by `./scripts/smoke.sh` as part of the existing run.
- **Manual measurement** on the reference hardware for any PR claiming a perf-relevant change.
- **CI gates** for startup time, full-test-suite time, and coverage.

## Adjusting a Budget

Budgets are not aspirational; they reflect what the system can actually do. When a budget is wrong:

1. Open a PR that **changes the budget** in this document.
2. State the reason and the new value.
3. Include the measurement that justifies it.
4. Update any tests that enforce it.

Adjusting a budget in the same PR that breaks it is permitted **only** with explicit justification in the PR description.

## Profile-Specific Budgets

### `local-lite`

Same hardware assumption (a developer laptop), but with all services in one JVM and minimal observability. Latency budgets unchanged; throughput budgets relaxed to 50% of the local-full values.

### `cloud-cheap`

A single small VPS (e.g., 4 vCPU, 8 GB RAM). Latency budgets relaxed by 2×; throughput by 50%.

### `production-reference`

Aspirational. Budgets are aligned with the deployed Kafka cluster, Iceberg catalog, and Trino instance — defined when Phase 8 lands.
