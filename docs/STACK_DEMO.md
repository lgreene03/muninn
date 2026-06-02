# The Norse Stack — Full End-to-End Demo

A guided tour of the **whole four-service stack**, from a raw trade event to a
simulated portfolio fill, plus the research SDK that reads it all back out.

If you want the single-service deterministic-replay demo instead, see
[`DEMO.md`](DEMO.md). This document is the *stack-level* story: how the four
services compose into one pipeline.

```
            ┌─────────────┐   features    ┌──────────┐   intents    ┌────────────┐
 trades ───▶│   muninn    │──────────────▶│  huginn  │─────────────▶│  sleipnir  │───▶ exchange
            │ feature eng │  (Redpanda)   │ strategy │  (Redpanda)  │  exec gw   │   (Binance testnet)
            └─────────────┘               └──────────┘              └────────────┘
                   ▲                            ▲      ◀── fills ──────────┘
                   │                            │      (Redpanda)
            ┌──────┴───────┐            ┌────────┴────────┐
            │  muninn-py   │            │   web dashboard │
            │  SDK / CLI   │            │  (operator UI)  │
            └──────────────┘            └─────────────────┘
```

| Service | Language | Role | Local port |
|---|---|---|---|
| [muninn](https://github.com/lgreene03/muninn) | Java / Spring Boot | Deterministic feature engine — ingest → compute → query | `8080` |
| [muninn-py](https://github.com/lgreene03/muninn-py) | Python | Research SDK, CLI, Streamlit dashboard | (library) |
| [huginn](https://github.com/lgreene03/huginn) | Go | Strategy engine — consumes features, emits order intents | `8083` |
| [sleipnir](https://github.com/lgreene03/sleipnir) | Go | Execution gateway — submits intents, reports fills | `8085` |

The naming is Norse: **Muninn** ("memory") observes and computes; **Huginn**
("thought") decides and acts; **Sleipnir** (Odin's steed) carries orders to the
venue and fills back.

---

## What this demo proves

By the end you will have watched a single trade flow the length of the stack:

1. A **trade** is ingested by Muninn over HTTP and lands on `events.trade`.
2. Muninn's **feature engine** computes a windowed VWAP feature, deterministically.
3. A **feature event** reaches Huginn, which runs a strategy and emits an **order intent**.
4. **Sleipnir** consumes the intent, runs pre-trade risk checks, "executes" it, and republishes a **fill**.
5. Huginn applies the fill to its **portfolio** and you can read the position back over HTTP.
6. The **muninn-py SDK** queries the computed feature back out in three lines of Python.

Two architectural invariants make this more than a toy:

- **One computation path.** Muninn's feature engine produces byte-identical
  output whether it reads from a live broker or replays history. (Proved by the
  `ReplayDeterminismIntegrationTest` and enforced by ArchUnit — see
  [`DEMO.md`](DEMO.md) §5.)
- **One wire contract.** The `ExecutionID` idempotency key and W3C
  TraceContext headers flow end-to-end across the Kafka boundary, so a single
  intent traces `huginn → sleipnir → huginn` as one span tree. See
  [`sleipnir/docs/CONTRACTS.md`](https://github.com/lgreene03/sleipnir/blob/main/docs/CONTRACTS.md).

---

## Prerequisites

- **Docker** (Docker Desktop, OrbStack, or Colima).
- Sibling checkouts next to this repo: `../huginn` and `../sleipnir`.
- `curl` and `jq` for poking at the APIs.
- ~6 GB free disk for the three image builds plus container working sets.

---

## Quickstart — the whole pipeline in one command

The fastest path is the bundled cross-stack smoke test, which boots all four
services, pushes a trade, drives a strategy signal, and asserts the round-trip:

```bash
cd muninn
bash scripts/smoke-stack.sh             # leave the stack up for exploration
#                          --teardown    # …or tear it all down when done
```

A green run ends with:

```
═══════════════════════════════════════════════════
  Norse Stack smoke test passed (0 failures)
═══════════════════════════════════════════════════
```

It uses [`docker-compose.stack.yml`](../docker-compose.stack.yml), which builds
`muninn`, `huginn`, and `sleipnir` from source and wires them to a shared
Redpanda, two PostgreSQL instances, and MinIO. The rest of this document walks
the same path by hand so you can see each hop.

---

## Step 1 — Boot the stack

```bash
docker compose -f docker-compose.stack.yml up -d --build
# Wait for health:
curl -s localhost:8080/actuator/health   # muninn  → {"status":"UP"}
curl -s localhost:8083/healthz           # huginn  → portfolio snapshot
curl -s localhost:8085/healthz           # sleipnir → OK
```

Create the topics the pipeline uses (Redpanda auto-creates most, but be explicit):

```bash
docker compose -f docker-compose.stack.yml exec -T redpanda \
  rpk topic create events.trade features.obi.v1 features.vwap.1m.v1 \
  executions.intents.v1 executions.fills.v1
```

---

## Step 2 — Ingest a trade into Muninn

Muninn's ingestion endpoint takes a flat, validated `TradeEvent`. (This is the
contract the muninn-py SDK and `scripts/smoke.sh` both use — `POST
/api/v1/events/trade`, **not** a nested envelope.)

```bash
curl -s -X POST localhost:8080/api/v1/events/trade \
  -H 'Content-Type: application/json' \
  -d '{
    "eventId": "0193a8f0-0000-7000-8000-000000000001",
    "eventTime": "2026-06-03T12:00:00Z",
    "ingestTime": "2026-06-03T12:00:00Z",
    "source": "stack-demo",
    "instrument": {
      "symbol": "BTC-USDT", "baseAsset": "BTC", "quoteAsset": "USDT",
      "exchange": { "id": "binance", "displayName": "Binance Spot", "timezone": "UTC" }
    },
    "sequenceNumber": 1, "schemaVersion": 1,
    "price": 67500.50, "size": 0.01, "side": "BUY",
    "exchangeTradeId": "demo-001"
  }'
# → 201 {"eventId":"…","status":"accepted","topic":"events.trade"}
```

**What just happened (Muninn's features):**
- The event was validated (`EventValidator`) and published to `events.trade`.
- The **feature engine** (`FeatureEngineRunner`) consumes it under a watermark
  windowing model and computes a rolling **VWAP** (`VwapComputer`), checkpointing
  state to MinIO and emitting onto `features.vwap.1m.v1`.
- Invalid events are dead-lettered, not dropped silently.

---

## Step 3 — Query the feature back out

### Over HTTP

```bash
curl -s "localhost:8080/api/v1/features/vwap.1m?instrument=BTC-USDT\
&start=2026-06-03T00:00:00Z&end=2026-06-03T23:59:59Z" | jq
# → {"values":[ … ]}   (empty until a window closes)
```

List what features are registered:

```bash
curl -s localhost:8080/api/v1/features | jq
# → [{"name":"vwap.1m","version":"v1","outputKind":"VWAP","windowDuration":"00:01:00", …}]
```

> Note the contract: time bounds are `start`/`end` (+ optional `limit`), and the
> response is a `{"values":[…]}` envelope. The feature catalog lives at the bare
> `GET /api/v1/features`.

### From Python (muninn-py SDK)

The whole point of the SDK is that a researcher pulls features in a few lines:

```python
from muninn import MuninnClient

with MuninnClient(host="http://localhost:8080") as m:
    defs = m.list_features()                       # discover what's computed
    df = m.get_feature("vwap.1m", instrument="BTC-USDT",
                        start="2026-06-03T00:00:00Z", end="2026-06-03T23:59:59Z")
    print(df)                                      # a Polars DataFrame
```

**SDK features:** sync + async clients, parallel multi-feature `get_features`,
multi-instrument `get_panel`, a `.pandas` accessor, notebook helpers
(`forward_returns`, `information_coefficient`, …), retry/backoff, optional disk
cache, a `muninn` CLI, and a Streamlit dashboard (`muninn dashboard`).

```bash
muninn features list                  # CLI: table or --json
muninn replay submit --help           # submit deterministic replay jobs
```

---

## Step 4 — Drive a strategy signal through Huginn

Muninn computes VWAP; Huginn's bundled strategies key off order-book-imbalance
(OBI) features. The smoke test bridges this by injecting a synthetic OBI feature
onto the topic Huginn watches:

```bash
echo '{"eventId":"0193a8f0-0000-7000-8000-000000000002","eventTime":"2026-06-03T12:00:01Z",
"featureName":"obi","featureVersion":"v1","instrument":"BTC-USDT",
"windowStart":"2026-06-03T12:00:00Z","windowEnd":"2026-06-03T12:00:01Z",
"values":{"obi":-0.85,"micro_price":67500.50,"bid_price":67490.00,"ask_price":67510.00}}' \
| docker compose -f docker-compose.stack.yml exec -T redpanda \
    rpk topic produce features.obi.v1
```

**What just happened (Huginn's features):**
- The multi-topic **Kafka consumer** fanned the feature into the `Executor`.
- One of four strategies (`OBIThreshold`, `VPINBreakout`, `VWAPDeviation`,
  `EMACrossover`) implementing `OnFeature(FeatureEvent) → []Order` produced a signal.
- The **Risk Manager** checked it (drawdown stop, daily-loss limit, position
  limits, volatility-scaled sizing, staleness watchdog) before it became an intent.
- The intent was published to `executions.intents.v1`.

Inspect Huginn:

```bash
curl -s localhost:8083/api/snapshot | jq      # live portfolio + PnL
curl -s localhost:8083/metrics | grep huginn_ # Prometheus metrics
```

---

## Step 5 — Execute through Sleipnir and apply the fill

Sleipnir consumes the intent, runs **pre-trade risk** (size, notional, daily
counts, token-bucket rate limit), submits to the exchange (Binance Spot testnet,
or the in-memory **simulator** connector with `--exchange=sim`), and republishes
a verified `ExecutionFill` — carrying a stable `ExecutionID` so Huginn can't
double-count — onto `executions.fills.v1`.

Huginn ingests the fill, applies it to the FIFO-average-cost portfolio, and
journals it (JSONL or Postgres). Read the position back:

```bash
curl -s localhost:8083/api/snapshot | jq '.Positions, .TotalFills'
```

A non-zero `TotalFills` with an open position means a single trade has now
traversed **all four services** and come to rest as portfolio state.

---

## Step 6 — Observe the whole thing

The stack ships operational surfaces, not just APIs:

| Surface | Where |
|---|---|
| Muninn OpenAPI / Swagger | `localhost:8080/swagger-ui.html`, `localhost:8080/api-docs` |
| Muninn Prometheus metrics | `localhost:8080/actuator/prometheus` |
| Huginn operator dashboard (React) | `localhost:8084` (when the `dashboard` service is enabled) |
| Huginn SSE live stream | `localhost:8083/api/stream` |
| Sleipnir metrics + alerts | `localhost:8085/metrics` |
| Grafana / Prometheus / Tempo | via `docker-compose.observability.yml` |

Each service emits OpenTelemetry trace spans with W3C TraceContext propagated
through Kafka headers, so one intent renders as a single trace across the
`huginn → sleipnir → huginn` boundary in Jaeger/Tempo.

---

## Tear down

```bash
docker compose -f docker-compose.stack.yml down -v
```

---

## Where to go next

- **Determinism, the load-bearing claim:** [`DEMO.md`](DEMO.md) §5 and
  [`docs/steering/DETERMINISTIC_REPLAY.md`](steering/DETERMINISTIC_REPLAY.md).
- **Per-service deep dives:** each repo's `README.md` and `docs/ROADMAP.md`.
- **The wire contracts that hold the stack together:**
  [`sleipnir/docs/CONTRACTS.md`](https://github.com/lgreene03/sleipnir/blob/main/docs/CONTRACTS.md).
- **Research workflows:** the muninn-py docs site and bundled notebooks
  (`alpha_backtest_demo.ipynb`, `feature_drift_monitoring.ipynb`).
