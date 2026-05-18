# Talk proposal: One Computation Path

*Conference target: any streaming-systems or research-infrastructure venue (Strange Loop, QCon, KubeCon Data Day, Current). Length: 30 minutes. Speaker: project maintainer.*

## Title

**One Computation Path: Making Streaming Analytics Deterministically Reproducible**

## Tagline (one-line submission field)

How to build a streaming feature engine where the live and historical pipelines aren't *similar* — they're literally the same code, and a CI test enforces that they produce byte-identical outputs.

## Abstract (≤ 250 words)

Quantitative research and streaming-analytics teams have a chronic correctness problem: the live system and the historical system rarely agree. The pipeline that ran in production yesterday is not the pipeline that ran in your backtest last week. Numbers drift. Trust erodes. Eventually no one is sure whether an alert is a real signal or a divergence between paths.

The conventional response — make both paths "more similar" through a unified job model, careful type discipline, lateness-policy alignment — works in proportion to the engineering effort spent, and never quite finishes. It can't, because the two paths are conceptually different programs.

This talk presents a structural alternative implemented in Muninn, an open-source event-native feature computation platform. There is one feature engine. It accepts events from an `EventSource` abstraction. Two implementations exist — a live Kafka consumer and a seek-by-timestamp historical reader — and the engine cannot tell them apart. A purity discipline (no wall-clock reads, no random, no external IO in computation code; enforced at build time by ArchUnit) means the only inputs to any computation are the events and the prior state. A CI integration test produces a known event sequence, runs the engine live, replays it, and asserts byte-identical outputs.

We'll walk through the architecture, the build-time enforcement, the divergence-detection observability, and the deliberate scope of the claim (the one footnote is documented as ADR-0002). The session ends with a live demo running on a Mac mini.

## Key claims and where each is backed

| Claim | Backing |
|---|---|
| "Two paths share the same code." | `src/main/java/io/muninn/feature/engine/FeatureEngineRunner.java` — single class, one `run()` method. Two `EventSource` implementations under `feature.engine`. |
| "Build-time enforcement of purity." | `src/test/java/io/muninn/architecture/ArchitectureRulesTest.java` — nine ArchUnit rules including `no_wall_clock_in_feature_compute`, `no_random_in_feature_code`. |
| "CI asserts byte-identical replay." | `src/test/java/io/muninn/replay/ReplayDeterminismIntegrationTest.java` — runs on every push to `main`. |
| "Continuous shadow-replay divergence detection." | `src/main/java/io/muninn/replay/ShadowReplayComparator.java` — `@KafkaListener` against live + replay topics; emits `muninn.replay.divergence.detected` metric. |
| "Honest scope: `eventId` differs across runs by design." | [`docs/adr/0002-event-id-determinism.md`](../adr/0002-event-id-determinism.md). |
| "Local-first deployment footprint." | `docker-compose.yml` + `docs/steering/LOCAL_FIRST_CONSTRAINTS.md` — runs on a Mac mini M4 with 24 GB RAM. Reference hardware is the speaker's actual machine. |
| "Scales to cloud without rewriting." | `local-infra/terraform/aws/` + `deploy/helm/muninn/` + ADRs 0005–0007 — same code paths, profile-driven backend selection. |

Every claim above is grounded in a file path the audience can read. Nothing in the talk relies on benchmark numbers the project hasn't measured, and nothing claims comparative superiority over named alternatives.

## Outline (30 minutes)

1. **The drift problem (4 min).** A composite of three streaming-systems-correctness incidents I've personally seen or heard about. Concrete examples: a moving-average backtest that diverged from production due to a rounding step that lived in only one path; an alert that fired only in live because of a wall-clock-based stale-event filter that didn't exist in batch. Frame: this is not a problem of careless engineering. It's structural.
2. **The unconventional response (3 min).** Don't make the paths more similar. Make them the same path. State the architectural claim.
3. **What the code looks like (6 min).** Live walkthrough of `FeatureEngineRunner` + `EventSource` + `VwapComputer`. The `topicResolver` constructor parameter as the only thing that varies. The `mode` metric tag for observability.
4. **Why purity is the lever (5 min).** Common non-determinism sources: wall-clock, random seeds, `HashMap` iteration, floating-point ordering, external lookups. Each one mapped to a Muninn defense. ArchUnit rules shown on screen.
5. **The proof (4 min).** Live screen of `ReplayDeterminismIntegrationTest` running. The shadow comparator pipeline diagram. `muninn.replay.divergence.detected = 0` in Grafana.
6. **The honest scope (3 min).** ADR-0002. What this doesn't fix (code-version differences are correctly different; external IO must be pre-materialized; non-deterministic *upstream* sources are out of our control).
7. **Live demo (3 min).** `./scripts/demo.sh` on the laptop. End on the determinism panel reading zero.
8. **Q&A (2 min reserved within the slot).**

Slides are minimal — almost everything is real code, real terminal, real dashboards. The one diagram worth a slide is the architecture flow (already exists in the repo's `README.md`).

## Demos and dependencies

Live demos require:

- The speaker's laptop with Muninn + Docker Compose pre-warmed.
- A wired conference network connection or hotspot — public conference Wi-Fi has dropped me mid-demo before.
- A 5-minute pre-recorded fallback of the same flow, kept open in another tab.

No external APIs in the live demo path. The Binance feed is disabled and synthetic trades are used so the demo is reproducible regardless of network state.

## Audience and prerequisites

Target: engineers operating or building streaming-analytics systems. Some familiarity with Kafka or a comparable broker is assumed. No Java-specific knowledge needed — most of the code is small enough to follow visually.

This is not an introduction to streaming systems. It's an experience report on a specific architectural choice and what was needed to make it credible.

## Why me, why now

I'm the maintainer of Muninn — an open-source project built in public as both working infrastructure and a portfolio artifact. The project explicitly is not a trading bot, not a SaaS, and not for sale; it's an honest exploration of one architectural idea: one computation path, enforced by code. I have working code, a CI build that proves the property continuously, and a demo I can run on stage.

The "why now" is that streaming-analytics systems have proliferated faster than the discipline around them has. Most teams I talk to have the drift problem and don't have a structural answer. This talk is an attempt to give them one.

## Submission anti-patterns I will explicitly avoid

- No "disrupting" or "revolutionary" framing. The project is one architectural choice; many have made similar choices in their own systems. The value is in showing how to make the choice testable.
- No comparison slides naming Flink, Kafka Streams, Materialize, etc. as inferior. They aren't; they're solving different problems. I'll mention them only if a question requires it.
- No vanity numbers. The project hasn't been benchmarked at scale; I won't pretend it has.
- No crypto-hype framing. Crypto APIs are a convenient public data source for the demo. The same architecture serves IoT telemetry or server metrics.
- No reference to "AI" anywhere in the talk except to disclose that drafts of project documentation were AI-assisted, in the spirit of how the rest of Muninn's docs are written.

## Submission package

- **Title**, **tagline**, **abstract**: as above.
- **Speaker bio:** 80 words, attached separately to each venue's submission form.
- **Recording:** A 90-second teaser video showing the demo end-to-end. Hosted alongside the repo.
- **Slides:** PDF, minimal. Most of the talk is live code and real dashboards.
- **References:** Repo URL, ADR-0002 link, blog post link, Anthropic's multi-agent research engineering post (cited once as a structural-engineering source, not a topical one).

---

*Status: draft. Not yet submitted to any conference. Ready to be tailored per venue.*
