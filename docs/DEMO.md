# Muninn — 10-Minute Demo

A runnable walkthrough that gets a fresh clone from "nothing" to "deterministic replay verified" in under ten minutes on a local machine.

If you only have five minutes, the [Determinism Property](#section-5--the-determinism-property) section at the end is the load-bearing point — feel free to skim the setup.

---

## What this demo proves

By the end of this walkthrough you will have:

1. **Booted the full local stack** — Redpanda, PostgreSQL, MinIO, Prometheus, Grafana, Tempo — under Docker Compose.
2. **Ingested ten synthetic trades** via the HTTP ingestion API.
3. **Watched the feature engine compute a rolling VWAP** from those trades, live.
4. **Queried that VWAP back out** of the system via the Query API.
5. **Submitted a replay job** over the same event range.
6. **Observed `muninn.replay.divergence.detected == 0`** — the replay produced byte-identical outputs to the live run.

Step 6 is the load-bearing demonstration. Everything else is plumbing in service of it. See [`docs/steering/DETERMINISTIC_REPLAY.md`](steering/DETERMINISTIC_REPLAY.md) for what the property means and [`docs/adr/0002-event-id-determinism.md`](adr/0002-event-id-determinism.md) for the scope of the claim (`eventId` is provenance metadata; everything else is byte-identical).

## Prerequisites

- **Docker** (Docker Desktop, OrbStack, or Colima — anything that exposes the Docker socket).
- **Java 21** on `PATH`.
- **Maven** (the wrapper isn't currently checked in).
- **curl** and **jq** for inspecting API responses.
- **5 GB of free disk** for image pulls plus container working sets.

Reference hardware is a Mac mini M4 with 24 GB RAM. Any current x86 or ARM workstation works. See [`docs/steering/LOCAL_FIRST_CONSTRAINTS.md`](steering/LOCAL_FIRST_CONSTRAINTS.md) for the contract.

---

## Section 1 — Boot the stack (≈ 90 seconds warm, 5 min cold)

```bash
git clone https://github.com/lgreene03/muninn.git
cd muninn

# Bring up Redpanda, PostgreSQL, MinIO, Prometheus, Grafana, Tempo.
docker compose up -d --wait

# Create the Kafka topics the system expects.
./scripts/create-topics.sh
```

You should see:

```
✓ Created topic: events.trade
✓ Created topic: events.book.snapshot
✓ Created topic: events.candle
✓ Created topic: events.deadletter
✓ Created topic: features.vwap.1m.v1
✓ Created topic: features.vwap.1m.v1.replay
```

At this point the infrastructure is running but the Muninn JVM application is not yet started.

### What just launched

| Service | Local port | Purpose |
|---|---|---|
| Redpanda | `19092` | Kafka-compatible broker; the event log |
| Redpanda Console | `8088` | Web UI for inspecting topics |
| PostgreSQL | `5433` | Metadata (replay jobs, feature definitions, reference data) |
| MinIO | `9000` / `9001` | S3-compatible warehouse for Parquet |
| Prometheus | `9091` | Metrics scrape and alert evaluation |
| Grafana | `3001` | Dashboards |
| Tempo | `3200` | Distributed tracing |

## Section 2 — Start the Muninn application

Open a second terminal:

```bash
mvn spring-boot:run
```

Wait until the log shows:

```
Started MuninnApplication in 4.2 seconds
Feature engine started feature="vwap.1m" windowDuration="PT1M"
Feature query service ready backend="duckdb"
Feature archival consumer ready sink="parquet"
```

Health-check it:

```bash
curl -s http://localhost:8080/actuator/health | jq .
```

Expected: `{"status":"UP", ...}`.

## Section 3 — Run the demo script

Back in your original terminal:

```bash
./scripts/demo.sh
```

The script does five things end-to-end:

1. **Injects ten synthetic `TradeEvent`s** for `BTC-USDT` via `POST /api/v1/events/trade`, half a second apart.
2. **Queries the rolling VWAP** back out via `GET /api/v1/features/vwap`.
3. **Submits a replay job** for the same event-time window via `POST /api/v1/replay/jobs`.
4. **Polls the job status** until it reaches `COMPLETED`.
5. **Prints the telemetry URLs** for visual inspection.

Concrete expected output (timestamps and IDs differ on each run):

```
→ Step 1: Injecting 10 sequential synthetic trade events...
✓ Posted Trade #1: Price=90000.00, Qty=1.0 -> Response: {"status":"accepted","eventId":"01923e..."}
... (nine more)

→ Step 2: Retrieving real-time computed rolling VWAP features via DuckDB...
Raw API Response: [{"window_start":"2026-05-18T22:10:00Z","window_end":"2026-05-18T22:11:00Z","vwap_value":"90000.0","event_count":10}]

→ Step 3: Triggering a historical shadow replay job for BTC-USDT...
✓ Replay job submitted successfully!
Replay Job Details: {"jobId":"01923e...","status":"PENDING", ...}

→ Step 4: Monitoring Replay Job Progress...
✓ Replay job completed successfully!
Final Job Status: {"status":"COMPLETED","eventsReplayed":10,"elapsed":"PT2.34S", ...}

═══════════════════════════════════════════════════════════════════
        Demo Run Completed Successfully! Live & Replay Matched
═══════════════════════════════════════════════════════════════════
```

If you see `COMPLETED`, the replay finished. Verifying that **live and replay outputs match byte-for-byte** is the next step.

## Section 4 — Inspect the telemetry

### Pipeline overview (Grafana)

Open <http://localhost:3001> (login `admin` / `admin`). Three dashboards are pre-loaded:

- **Pipeline overview** — ingest rate, broker lag, feature emission rate, replay-job counter.
- **Determinism panel** — `muninn.replay.divergence.detected` count over time, last successful audit timestamp.
- **Resource panel** — per-container memory and CPU vs. caps.

After running the demo, the determinism panel's divergence counter should read **0**. That's the proof — see Section 5.

### Topic inspection (Redpanda Console)

Open <http://localhost:8088>. Two interesting topics:

- `features.vwap.1m.v1` — what the **live** engine produced.
- `features.vwap.1m.v1.replay` — what the **replay** engine produced for the same input range.

Compare two messages from the two topics that share a `windowStart`. The computational fields (`featureName`, `featureVersion`, `windowStart`, `windowEnd`, `value`, `inputEventIds.size()`, `codeVersion`) are identical. Only `eventId` and `processingTime` differ — both excluded from the determinism claim by [ADR-0002](adr/0002-event-id-determinism.md).

### Distributed trace (Tempo)

Open <http://localhost:3001/explore?orgId=1&left=%7B%22datasource%22:%22tempo%22%7D>. Search by service `muninn`. Pick any trace; you'll see the ingestion → engine → archival path as a connected span tree, with the `eventId` propagated end-to-end.

## Section 5 — The determinism property

The `muninn.replay.divergence.detected` counter being **zero** isn't a smoke test of "did it run". It's an architectural invariant.

The `ShadowReplayComparator` subscribes to both `features.vwap.1m.v1` (live) and `features.vwap.1m.v1.replay`. For every `(featureName, featureVersion, windowStart)` key, it pairs up the live and replay events and feeds them to `ReplayDivergenceDetector.compare(...)`. That comparator checks:

- `windowStart` equality
- `windowEnd` equality
- `value` equality (via `BigDecimal.compareTo`, so `60000` and `60000.00` are equal as numbers but not as strings)
- `inputEventIds.size()` equality

If any pair disagrees on any of those, the counter increments and an `ERROR` log line names the field and both values. The pre-loaded Prometheus alert rule fires within 5 minutes.

You can confirm this is real, not vapor:

```bash
# Look at the ArchUnit rule that forbids non-determinism patterns in feature code.
grep -A 5 "no_wall_clock_in_feature_compute" src/test/java/io/muninn/architecture/ArchitectureRulesTest.java

# Run the determinism integration test directly.
mvn -B verify -Dit.test=ReplayDeterminismIntegrationTest
```

The integration test produces 6 trades, runs the engine, replays them, and asserts byte-equality on the computational fields. It's part of every CI run on `main` — the green badge on the README is enforcing this property continuously.

## Section 6 — Going further

### Pull features into a notebook

The Python SDK at <https://github.com/lgreene03/muninn-py> reads from the running Query API:

```python
from muninn import MuninnClient

with MuninnClient() as m:
    df = m.get_features(
        instrument="BTC-USDT",
        features=["vwap.1m"],
        start="2026-05-18T22:00:00Z",
        end="2026-05-18T23:00:00Z",
    )
    df.head()
```

The bundled notebook (`notebooks/alpha_backtest_demo.ipynb` in the SDK repo) walks through pulling multiple features, computing forward returns, and submitting a replay to close the loop. Same code that works against your local stack works against a `production-reference` deployment by changing one URL.

### Try a Binance live feed

`muninn.ingestion.binance.enabled=true` in `application.yml` connects to the real Binance public WebSocket. After a few minutes of trade flow, query the VWAP for `BTC-USDT` and watch it move.

### Deploy to a cloud

[`docs/DEPLOY.md`](DEPLOY.md) walks through `terraform apply` → `docker build & push` → `helm install` for the `production-reference` profile. Same Helm values flag flips `query.backend=trino` and `feature.archivalSink=iceberg`; the same code that ran locally now runs against MSK + Iceberg + Trino.

## Tear-down

```bash
docker compose down -v
```

Removes containers and volumes. The repo is unchanged.

## Reading order if you want to understand the architecture

1. [`README.md`](../README.md) — the 30-second hook and the architecture diagram.
2. [`docs/steering/PROJECT_CONTEXT.md`](steering/PROJECT_CONTEXT.md) — what problem this solves.
3. [`docs/steering/DETERMINISTIC_REPLAY.md`](steering/DETERMINISTIC_REPLAY.md) — the central technical claim.
4. [`docs/steering/ARCHITECTURE_PRINCIPLES.md`](steering/ARCHITECTURE_PRINCIPLES.md) — the ten load-bearing principles.
5. [`docs/adr/`](adr/) — the seven Architecture Decision Records covering Iceberg, Trino, MSK, EKS, the eventId scoping decision, and the ADR process itself.
6. [`docs/steering/NON_GOALS.md`](steering/NON_GOALS.md) — what we deliberately won't build.

Thirty minutes of reading in that order will let you explain Muninn's architecture without help.
