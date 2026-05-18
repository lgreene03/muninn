# Muninn Screencast — Outline

5-minute screencast script. Audience: engineers familiar with streaming systems who landed on the repo from HN, a conference talk, or a colleague's link. Goal: make the determinism property visible and credible in five minutes.

The narration is written for a calm, technical voice. No salesy adjectives. No "revolutionary". Numbers and demos over claims.

## Open

**Shot:** Terminal window, monospace font, dark background. Hands at rest.
**On-screen text:** None.
**Duration:** 5 seconds.
**Narration:**

> Muninn is event-native research infrastructure. The interesting claim is that any feature it computes live can be reproduced byte-for-byte by replay. In the next five minutes I'm going to show that's literally true, not aspirational.

## Beat 1 — The pitch in one diagram (30 s)

**Shot:** Cut to `README.md` open in a browser. The architecture ASCII diagram is visible. Cursor highlights the two parallel paths: `feature-engine (live)` and `replay-engine (historical)` both feeding the same downstream artifacts.
**On-screen text:** None.
**Duration:** 30 seconds.
**Narration:**

> Two paths share the same code. The feature engine that emits a value as a trade arrives is the same engine, the same window logic, the same checkpoint format, that the replay engine runs against the historical log. There's one class. The difference is the source — a Kafka consumer for live, or a seek-by-timestamp reader for replay. Nothing branches on which.

## Beat 2 — Boot (45 s)

**Shot:** Back to terminal. Run `docker compose up -d --wait` and `./scripts/create-topics.sh`. Skip ahead through the compose-up output if it's slow.
**On-screen text:** Time-elapsed counter top-right.
**Duration:** 45 seconds compressed.
**Narration:**

> Local-first. The whole stack runs in Compose on a Mac mini. Redpanda for the event log, MinIO for warehouse storage, Postgres for metadata, Prometheus and Grafana for observability. Boot's about ninety seconds warm.

While compose boots:

> The project is also explicitly *not* a trading bot. Crypto exchange APIs are a convenient public data source. The same architecture runs against IoT telemetry, server metrics, anything event-native.

## Beat 3 — Send trades (30 s)

**Shot:** Run `mvn spring-boot:run` in a second pane. Wait for "Started MuninnApplication". Then run `./scripts/demo.sh` and watch ten `Posted Trade #N` lines stream by.
**On-screen text:** None.
**Duration:** 30 seconds.
**Narration:**

> Ten synthetic trades posted to the ingestion API. Each one validates against a canonical schema, gets stamped with a UUIDv7, and lands in the event log. From there the feature engine consumes them in event-time order.

## Beat 4 — Live VWAP (40 s)

**Shot:** Demo script reaches Step 2; the GET to `/api/v1/features/vwap` returns. Cursor highlights the `vwap_value` and `event_count` fields.
**On-screen text:** "Query API → DuckDB → Parquet warehouse".
**Duration:** 40 seconds.
**Narration:**

> The query API answered. That VWAP came from the live engine processing the trades, writing Parquet to MinIO, and DuckDB scanning the warehouse on the read side. Same architecture as if this were a year's worth of data; nothing's special-cased for "recent".

Brief pause, then:

> Now the interesting part.

## Beat 5 — Replay (45 s)

**Shot:** Demo script Step 3 — `POST /api/v1/replay/jobs`. Spinner advances. Step 4 reaches `COMPLETED`. Show the JSON response.
**On-screen text:** "Same event range → fresh feature-engine instance → `.replay` topic".
**Duration:** 45 seconds.
**Narration:**

> A replay job for the same event-time range. The replay engine spins up a fresh feature-engine instance — same class, same windowing, same checkpoint format — and points it at the historical log via seek-by-timestamp. Outputs land on a sibling topic.

Pause for `COMPLETED`:

> Done. Now the shadow comparator has been listening to both topics the whole time.

## Beat 6 — The proof (60 s)

**Shot:** Switch to Grafana. Open the **Determinism panel**. Point at the `muninn.replay.divergence.detected` graph — flat at zero.
**On-screen text:** "muninn.replay.divergence.detected == 0".
**Duration:** 60 seconds.
**Narration:**

> Zero divergences. For every window the live engine produced, the replay engine produced a matching output — same `windowStart`, `windowEnd`, same `BigDecimal` value, same number of input events. The comparator runs `BigDecimal.compareTo`, not string equality, so `60000` and `60000.00` register as equal.

Cut to terminal:

> The ArchUnit rules in the test suite forbid the patterns that would break this. No `Instant.now()` in the feature-computation packages. No unseeded random. No HashMap iteration for output ordering. The rules are enforced at build time. Every PR is checked.

Show `grep -A 5 no_wall_clock_in_feature_compute src/test/java/io/muninn/architecture/ArchitectureRulesTest.java`.

> And there's an integration test that does exactly what you just saw — six trades, run live, replay, assert byte-identical — that runs on every CI build. The green badge on the README is enforcing the determinism property continuously.

## Beat 7 — Why this matters (40 s)

**Shot:** Back to README, scroll to the "Problem" section.
**On-screen text:** None.
**Duration:** 40 seconds.
**Narration:**

> Quantitative research has a chronic correctness problem: the code that develops a feature isn't the code that runs it in production. Notebooks run on cleaned CSVs. Production runs on a stream. The two diverge silently. Backtests pass; deployments fail; nobody can tell what actually happened.

> Muninn fixes that by making it impossible for the two to diverge. There aren't two engines. The replay you just watched isn't a separate code path that mimics production — it *is* the production code path, running over historical events.

## Close (15 s)

**Shot:** README hero diagram, then transition to the GitHub repo URL.
**On-screen text:** "github.com/lgreene03/muninn".
**Duration:** 15 seconds.
**Narration:**

> Muninn is at github.com/lgreene03/muninn. The Python SDK for notebook research is at muninn-py. There's a DEMO.md that runs everything you just saw, and a blog post that walks through how the determinism is enforced. Thanks for watching.

---

## Production notes

- **Total wall-clock:** ~5 minutes 30 seconds at this pacing. Compressing compose-up to 10s buys flexibility if a beat runs long.
- **Audio:** Single narrator. No music bed. A subtle ticking-cursor or terminal-tap sound during command execution helps pace.
- **Fonts:** JetBrains Mono / Berkeley Mono in the terminal at 16-18pt. Browser zoom at ~125% so the README is readable.
- **Caption track:** Generate from narration; review for "Muninn" vs "Munin" spelling (the second `n` matters; named after the Norse raven).
- **Filming environment:** Quiet room. Mac mini reference machine for the demo so resource panels read realistically; avoid filming on a 96-core workstation that hides the local-first claim.

## What to cut if the runtime is over

Beat 7 ("Why this matters") is optional. The demo itself — beats 2 through 6 — is the load-bearing content. If the screencast runs long, trim Beat 1 from 30s to 15s and skip Beat 7 entirely.

## What NOT to add

- No music bed. The video is for engineers; music would feel salesy.
- No claims about performance ("processes millions of events per second") — the project hasn't measured that and won't say it.
- No frames showing a hypothetical UI dashboard mocked up in Figma. The Grafana panels are real; that's the point.
- No comparison slides ("Muninn vs Kafka Streams"). Out of scope; mentioned in passing only if a viewer asks.
