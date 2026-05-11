# OBSERVABILITY_STRATEGY.md

Observability is not a feature added at the end. It is part of every service from its first commit. A change that ships without telemetry is incomplete.

## The Three Pillars

### Structured Logs

- **Format:** JSON via `logstash-logback-encoder`.
- **Required fields:** `timestamp`, `level`, `logger`, `message`, `service`, `traceId`, `spanId`, `eventId` (when applicable).
- **Levels:**
  - `ERROR`: an action failed and a human should be notified.
  - `WARN`: an unexpected condition was handled; investigate if frequent.
  - `INFO`: significant state changes (service started, replay job completed).
  - `DEBUG`: dev-time detail, off in `local-full` and above by default.
  - `TRACE`: never enabled in any persistent deployment.
- **Forbidden:** string concatenation of context. Use `MDC` or structured key/value pairs.

### Metrics (Micrometer → Prometheus)

Every service exposes `/actuator/prometheus`. Standard JVM, HTTP, and Kafka metrics are emitted automatically by Spring Boot. **Application metrics** below are added explicitly.

### Traces (OpenTelemetry → Tempo/Jaeger)

Spring Boot 3 auto-instruments HTTP, Kafka, JDBC, and S3 calls. Custom spans wrap:

- Each event's path through ingestion → feature engine → output.
- Each replay job.
- Each query-API request.

Sampling: 100% in `local-lite` and `local-full`; 10% in `cloud-cheap`; head-based sampling tuned per service in `production-reference`.

---

## Application Metrics (Required)

These metrics are part of the system contract. They are named, typed, and labeled consistently.

### Ingestion

| Metric                                | Type      | Labels                              | Meaning                                          |
|---------------------------------------|-----------|-------------------------------------|--------------------------------------------------|
| `muninn.ingest.events.total`          | Counter   | `source`, `event_type`              | Events ingested                                  |
| `muninn.ingest.validation.failed`     | Counter   | `source`, `reason`                  | Events rejected at validation                    |
| `muninn.ingest.source.latency`        | Histogram | `source`                            | `ingestTime - eventTime`                         |
| `muninn.ingest.source.reconnects`     | Counter   | `source`                            | Source reconnection attempts                     |
| `muninn.ingest.lag.seconds`           | Gauge     | `source`, `instrument`              | Wall-clock seconds since last event from source  |

### Feature Engine

| Metric                                | Type      | Labels                              | Meaning                                          |
|---------------------------------------|-----------|-------------------------------------|--------------------------------------------------|
| `muninn.feature.events.processed`     | Counter   | `feature`, `version`                | Events consumed by the feature                   |
| `muninn.feature.outputs.emitted`      | Counter   | `feature`, `version`                | Outputs produced                                 |
| `muninn.feature.latency`              | Histogram | `feature`, `version`                | Event-time-of-emission − event-time-of-trigger   |
| `muninn.feature.processing.delay`     | Histogram | `feature`, `version`                | Processing-time delay (wall-clock)               |
| `muninn.feature.watermark.lag`        | Gauge     | `feature`, `partition`              | Wall-clock − watermark                           |
| `muninn.feature.late.events`          | Counter   | `feature`, `policy`                 | Events arriving below the watermark              |
| `muninn.feature.checkpoint.duration`  | Histogram | `feature`                           | Time to write a checkpoint                       |

### Replay

| Metric                                | Type      | Labels                              | Meaning                                          |
|---------------------------------------|-----------|-------------------------------------|--------------------------------------------------|
| `muninn.replay.jobs.active`           | Gauge     | (none)                              | Replay jobs in flight                            |
| `muninn.replay.job.duration`          | Histogram | `feature`, `status`                 | Wall-clock duration of a replay job              |
| `muninn.replay.events.processed`      | Counter   | `job_id`                            | Events processed by a replay job                 |
| `muninn.replay.divergence.detected`   | Counter   | `feature`, `version`                | Mismatches between live and shadow replay        |
| `muninn.replay.divergence.magnitude`  | Histogram | `feature`, `version`                | Diff magnitude when a divergence is detected     |

### Broker (Redpanda)

| Metric                                | Type      | Labels                              | Meaning                                          |
|---------------------------------------|-----------|-------------------------------------|--------------------------------------------------|
| `muninn.broker.lag.records`           | Gauge     | `consumer_group`, `topic`, `part`   | Consumer lag in records                          |
| `muninn.broker.lag.seconds`           | Gauge     | `consumer_group`, `topic`, `part`   | Consumer lag in event-time seconds               |

### Query API

| Metric                                | Type      | Labels                              | Meaning                                          |
|---------------------------------------|-----------|-------------------------------------|--------------------------------------------------|
| `muninn.query.requests`               | Counter   | `endpoint`, `status`                | Request count                                    |
| `muninn.query.latency`                | Histogram | `endpoint`                          | Request latency                                  |
| `muninn.query.duckdb.scan.bytes`      | Histogram | `endpoint`                          | Bytes scanned by DuckDB                          |

---

## Health Endpoints

Every service exposes Spring Boot Actuator endpoints:

- `/actuator/health` — liveness + readiness, with sub-checks for Redpanda connectivity, PostgreSQL connectivity, MinIO connectivity.
- `/actuator/info` — service version, git SHA, build time, active profile.
- `/actuator/prometheus` — metrics scrape.
- `/actuator/loggers` — runtime log-level adjustment.

`/actuator/health` returns `UP` only when every required dependency is reachable and the service is processing events (for stream-consuming services).

## Dashboards

The `local-full` profile ships with three Grafana dashboards:

1. **Pipeline overview.** Ingest rate, feature emission rate, broker lag, replay-job status.
2. **Determinism panel.** Divergence count, divergence magnitude, last successful nightly audit.
3. **Resource panel.** Per-container memory and CPU vs. caps from [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md).

Dashboard JSON is checked into `local-infra/observability/grafana/dashboards/`.

## Alerts

In `local-full` and above, alert rules are defined in Prometheus and surfaced in Grafana (no external alerting backend in MVP):

- **Critical:** `muninn.replay.divergence.detected` > 0 in the last 5 minutes.
- **Critical:** `muninn.ingest.lag.seconds` > 60 for any source.
- **Warning:** `muninn.feature.watermark.lag` > 30s for any feature.
- **Warning:** `muninn.broker.lag.records` > 10k for any consumer group.

## Anti-Patterns

- Counting things without labels. (A counter without dimensions is rarely useful.)
- Labels with unbounded cardinality (`user_id`, `event_id`, `instrument` when there are millions). Cap to known sets.
- Logging at `INFO` inside a hot loop.
- Emitting a metric without documenting it here.
