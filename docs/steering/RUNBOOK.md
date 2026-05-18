# RUNBOOK.md

Operational playbooks for Muninn. This document is for the operator running the system, not for the developer building features. When something goes wrong, find the matching section, follow the steps, and update this doc afterward if the steps were imperfect.

A runbook is only as good as its last incident. If you encountered a problem not covered here, add a section.

## How to Read a Symptom

Every section follows the same shape:

1. **Symptom** — what you see (a metric, an alert, a log pattern).
2. **What it means** — the underlying condition.
3. **Investigation** — how to confirm.
4. **Resolution** — what to do.
5. **Prevention** — what to change so it doesn't recur.

## General Triage Order

When something is wrong and you're not sure where to start:

1. Check `./actuator/health` on the affected service.
2. Check the Grafana dashboards at `http://localhost:3001` for real-time pipeline, determinism, and resource metrics.
3. Query Prometheus at `http://localhost:9091` to analyze metric trends or alert rules.
4. Trace transactions E2E in Grafana Tempo at `http://localhost:3200` (port `9411` for Zipkin collectors).
5. Tail structured logs filtered by `service` and the affected `traceId`.
6. Confirm dependencies are reachable: `docker compose ps`.

---

## Observability Stack

The telemetry infrastructure runs entirely local-first under Docker Compose, capped within a **1 GB memory limit**:
*   **Prometheus** (`http://localhost:9091`): Aggregates system and custom application metrics with a 5s scrape interval.
*   **Grafana** (`http://localhost:3001`): Rich, pre-provisioned data visualization.
*   **Grafana Tempo** (`http://localhost:3200`): High-performance distributed tracing collector accepting Zipkin-formatted traces on port `9411`.

### Pre-Provisioned Dashboards

1.  **Pipeline Overview** (`pipeline-overview`):
    *   **Ingestion Rate**: Live throughput rates of ingested trade events per second.
    *   **Ingestion Lag**: Dynamic delay (wall-clock − event timestamp) tracking ingestion pipeline staleness.
    *   **Feature Emission Rate**: Output rate of calculated features (VWAP, etc.) per second.
    *   **Watermark Lag**: Event-time processing latency (wall-clock − current engine watermark).
    *   **Active Replay Jobs**: Count of historical re-processing runs currently active.
2.  **Determinism Panel** (`determinism-panel`):
    *   **Divergences**: Rate of difference detections between live and replayed execution paths.
    *   **Divergence Magnitude**: Amplitude difference between mismatched computed floats.
3.  **Resource Panel** (`resource-panel`):
    *   **Memory**: JVM heap memory allocations (Used vs. Max).
    *   **CPU**: Host vs. application process CPU usage percentages.

### Alerting Rules and Triaging

#### Critical: `ReplayDivergenceDetected`
*   **Condition**: Divergence is detected (`muninn_replay_divergence_detected_total > 0`).
*   **Triage**:
    1. Open Grafana and view **Determinism Panel** to see the feature and version affected.
    2. Extract logs filtered by `level: ERROR` and look for key `reason: DIVERGENCE`.
    3. Note the mismatched computed values and `traceId` / `windowStart`.
    4. Run JFR profiling or local determinism test suite to identify side-effects.

#### Critical: `IngestionLagTooHigh`
*   **Condition**: Ingestion pipeline lag exceeds 60 seconds (`muninn_ingest_lag_seconds > 60`).
*   **Triage**:
    1. View the **Pipeline Overview** dashboard to isolate the source adapter (e.g., Binance).
    2. Check adapter connection metrics: `muninn_ingest_source_reconnects_total`.
    3. Verify network outbound connection to external exchanges.

#### Warning: `WatermarkLagWarning`
*   **Condition**: Watermark lag exceeds 30 seconds (`muninn_feature_watermark_lag > 30000`).
*   **Triage**:
    1. Check if ingestion lag is also high (watermark lag is a downstream symptom).
    2. If ingestion is fast but watermarks are slow, the engine is bottlenecked. View the **Resource Panel** for JVM CPU and GC pause time spikes.

#### Warning: `ConsumerGroupBrokerLag`
*   **Condition**: Kafka consumer group lag exceeds 10,000 records (`muninn_broker_lag_records > 10000`).
*   **Triage**:
    1. Identify the lagged consumer group from the alert labels.
    2. Restart the consumer instance or increase thread concurrency if processing is bottlenecked.

### Distributed Tracing (OTel)

All HTTP REST endpoints and internal message streams automatically propagate trace contexts.
*   **How to query a trace**:
    1. Copy a `traceId` from application structured logs or actuator headers.
    2. Go to Grafana (`http://localhost:3001`), choose **Explore**, and select the **Tempo** datasource.
    3. Paste the `traceId` into the query field to visualize the full execution span (Ingestion Web -> Kafka -> Feature Engine -> Storage).

---

## Ingestion

### Symptom: `muninn.ingest.lag.seconds` rising past 60 s

**What it means.** The ingestion adapter is not keeping up with the source feed, or the source feed has slowed down, or both.

**Investigation.**
- Is the WebSocket connected? Check `muninn.ingest.source.reconnects` for recent reconnects.
- Is validation failing in bulk? Check `muninn.ingest.validation.failed` rate.
- Is Kafka acknowledging? Check producer error logs and `muninn.broker.lag.records` for the producer side.

**Resolution.**
- If reconnects are climbing: the source is rate-limiting or dropping us. Back off, check exchange status page.
- If validation is failing: the source has changed its payload shape. Check the adapter parser; this is a schema-drift incident.
- If Kafka is slow: see "Broker" section below.

**Prevention.** Add a contract test against the source's last-known payload. Wire an alert at 30 s lag instead of 60 s to give earlier warning.

### Symptom: events arriving in `events.deadletter` with no obvious bad input

**What it means.** Validation is rejecting events that look legitimate. Almost always a schema-drift or clock-skew issue.

**Investigation.**
- Read 5 dead-letter messages with `rpk topic consume events.deadletter -n 5`.
- Look at the `reason` field on each.
- Common causes: `eventTime > ingestTime + skewTolerance`, missing required field, unparseable decimal.

**Resolution.**
- If clock skew: investigate the source's reported timestamps. Coinbase and Binance occasionally emit out-of-order data.
- If schema drift: update the adapter parser; add a golden fixture from the new payload.
- If genuinely bad data: the dead-letter is doing its job. Leave it.

**Prevention.** Tighten or loosen `clockSkewTolerance` in the validator config based on observed reality. Update the golden fixtures when sources change shape.

### Symptom: ingestion process restarts in a loop

**What it means.** A startup-time failure — usually a Kafka connection, a Flyway migration, or a malformed configuration.

**Investigation.** Read the first 50 lines of the latest restart's log.

**Resolution.**
- `Connection refused: redpanda:9092` → broker not up; `docker compose up -d redpanda`.
- `FlywayException` → migration conflict; do **not** delete the schema. Resolve by inspecting `flyway_schema_history`.
- `IllegalArgumentException` from `@ConfigurationProperties` → fix the malformed value in `application.yml` or env vars.

**Prevention.** Add a readiness probe that waits for Kafka before binding the producer.

---

## Feature Engine

### Symptom: `muninn.feature.watermark.lag` > 30 s

**What it means.** The feature engine's notion of event-time is more than 30 seconds behind wall-clock. Either events are slow to arrive, or one partition is starved.

**Investigation.**
- Is it global or per-partition? Filter the metric by the `partition` label.
- Is ingestion lag high (see above)? If yes, watermark lag is a downstream symptom.
- Is one partition silent? Check trade activity for the affected instrument on the source.

**Resolution.**
- Global lag mirroring ingestion → fix ingestion; this is downstream.
- Single partition silent → the source isn't producing events for that key. Investigate the source.
- Engine is processing slowly → check `muninn.feature.processing.delay`; if growing, the engine is CPU-bound. Profile with JFR.

**Prevention.** Set a per-partition liveness gauge that pages if no events arrive for a configurable window.

### Symptom: `muninn.replay.divergence.detected` non-zero

**What it means.** The shadow-replay produced a different output from the live path for the same input. This is the single most important alert in Muninn.

**Investigation.**
1. Note the `feature` and `version` labels.
2. Read the divergence-detection log line; it includes the diverging `featureName`, `windowStart`, and both values.
3. Pull both `FeatureComputedEvent`s from `features.live` and `features.replay` for that window.
4. Check `inputEventIds` on both — do they list the same events?

**Resolution.**
- **Same input, different output.** This is a code bug — non-determinism leaked into the feature. Find the source: a `now()` call, a `HashMap` iteration, an unseeded `Random`, a floating-point operation that should be `BigDecimal`. Add a regression test in `VwapDeterminismTest` (or the equivalent).
- **Different input.** Either replay is reading a different range, or events were lost between live and replay. Check broker retention and replay-job parameters.
- **Output-format change.** Someone updated the schema but didn't bump `featureVersion`. Bump the version; old outputs and new outputs are not comparable.

**Resolution is never "ignore it."** A divergence is always investigated.

**Prevention.** ArchUnit rules ([CODING_STANDARDS.md](CODING_STANDARDS.md) §Forbidden Patterns) catch most non-determinism at compile time. Strengthen them when a new pattern bites.

### Symptom: Engine restart loops

**What it means.** A startup failure — most often a corrupted checkpoint, a Kafka offset reset, or a feature-version mismatch.

**Investigation.** Read the first 50 lines of the restart log.

**Resolution.**
- `Checkpoint feature version mismatch` → the checkpoint was written by older code. Start from the previous checkpoint with matching version, or from `t=0`.
- `OffsetOutOfRangeException` → broker retention rolled the offsets the engine was tracking. Reset the consumer group to `earliest` and accept a re-process, or replay from a Parquet snapshot.
- `IOException` reading checkpoint from MinIO → MinIO is down or the bucket is gone. Recover MinIO; the engine can start from `t=0` if no checkpoint is reachable.

**Prevention.** Validate checkpoint versions at write time, not just at read time. Document the recovery path in operator notes.

---

## Broker (Redpanda)

### Symptom: `docker compose ps` shows redpanda is unhealthy or restarting

**What it means.** Redpanda is misconfigured, out of disk, or hitting a JVM/Seastar limit.

**Investigation.**
- `docker compose logs redpanda --tail 200`.
- `df -h` on the host; Redpanda dies fast when its volume is full.
- `docker stats redpanda` — is it actually hitting its memory cap?

**Resolution.**
- Disk full → roll old segments to MinIO; clean up `data/raw/` and `data/warehouse/` per [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md).
- Memory cap → temporarily raise in `docker-compose.yml`; investigate retention.
- Persistent corruption → as a last resort, `docker compose down -v` will wipe the broker. **This loses uncomitted events.** Only do this in development.

**Prevention.** Alert on `muninn.broker.lag.records` and on disk-usage of the Redpanda volume.

### Symptom: `muninn.broker.lag.records` climbing for a consumer group

**What it means.** A consumer is slower than the producer feeding it. Either the consumer is starved (CPU/IO) or the producer is bursting beyond steady state.

**Investigation.**
- Which consumer group? The label tells you.
- Is the consumer process up and reading? Check the service's metrics.
- Is the producer rate elevated? Compare against the throughput budget.

**Resolution.**
- Persistent: the consumer is under-provisioned. Profile and tune.
- Spiky: tune Kafka consumer fetch sizes; consider partitioning the topic.
- Stuck at a fixed offset: the consumer is crash-looping on a poison message. Find the offset; route to dead-letter manually if needed.

---

## Storage (MinIO + DuckDB)

### Symptom: query API returns 500s for historical ranges

**What it means.** DuckDB couldn't read a Parquet file from MinIO, or a partition is missing.

**Investigation.**
- Read the query-API log for the failing request; it logs the path and the DuckDB error.
- `mc ls local/muninn-warehouse/...` — does the partition exist?
- Try the query against MinIO directly to isolate.

**Resolution.**
- Missing partition → backfill via a replay job for that range.
- Corrupted Parquet → quarantine the file; rebuild from event log via replay.
- MinIO down → restart the container; check disk.

**Prevention.** Add a periodic integrity job that scans Parquet files for header corruption.

---

## PostgreSQL Metadata

### Symptom: Flyway migration fails on startup

**What it means.** Either a schema-history mismatch, a SQL syntax error in a new migration, or a permissions issue.

**Investigation.**
- Read the Flyway error message — it names the migration file and line.
- `psql -d muninn -c "select * from flyway_schema_history order by installed_rank desc limit 5"`.

**Resolution.**
- Syntax error in new migration → fix it in a follow-up commit. Never edit a migration that has already run in any environment.
- Out-of-order migrations → use `flyway repair` only if you understand exactly what state the schema is in.

**Prevention.** Migration files are immutable once merged. New schema changes go in new files. CI runs migrations against an empty database to catch ordering issues.

---

## Disaster Recovery

### Scenario: I lost the entire `data/` directory locally.

**Result.** All hot events (Redpanda) and warm archives (MinIO) are gone. PostgreSQL metadata is gone.

**Recovery.** Re-ingest from the source. Muninn does not have a backup of upstream feeds; the source is the ultimate source of truth. For production-reference, this is solved by S3 versioning and PostgreSQL snapshots.

### Scenario: The event log was correct, but feature outputs are wrong.

**Result.** Derived state was corrupted; raw events are fine.

**Recovery.** Delete feature outputs for the affected range. Run a replay job over the same range with the correct feature version. Outputs are recomputed bit-for-bit.

**This is the property the architecture is designed to deliver.** Use it.

### Scenario: A feature version was buggy and produced wrong outputs in production for a week.

**Recovery.**
1. Fix the bug. Bump `featureVersion`.
2. Replay the affected range with the new version. Outputs go to a separate topic / partition for the new version.
3. Consumers explicitly choose which version to read. Old (buggy) outputs remain in the log as a historical record — see [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md) §3.
4. Optionally, emit a `FeatureRetracted` event so downstream consumers know.

---

## Cloud Deployment (production-reference profile)

The `production-reference` profile runs on AWS — EKS + MSK + S3 + RDS + Glue. Architectural choices are in [ADR-0003](../adr/0003-managed-kafka-via-msk.md), [ADR-0004](../adr/0004-eks-over-fargate-only.md), [ADR-0005](../adr/0005-iceberg-with-glue-catalog.md). Fresh deploys follow [DEPLOY.md](../DEPLOY.md); migrations from local follow [PHASE8_MIGRATION.md](PHASE8_MIGRATION.md).

### Symptom: pods stuck in `CrashLoopBackOff` after Helm install

**What it means.** A required secret or ConfigMap is missing, or the pod can't reach MSK / RDS / S3.

**Investigation.**
- `kubectl describe pod -n muninn <pod>` — look at events.
- `kubectl logs -n muninn <pod> --previous` — last crash output.
- Confirm secrets exist: `kubectl get secrets -n muninn`.

**Resolution.**
- Secret missing → recreate per [DEPLOY.md §Step 4](../DEPLOY.md).
- MSK unreachable → check pod's security group is allowed by the MSK SG; confirm in-VPC routing.
- IAM permission denied (S3 / Glue) → IRSA annotation missing on the service account.

### Symptom: `muninn.broker.lag.records` rising on MSK but not on local Redpanda

**What it means.** The application's consumers in EKS are slower than producers, or MSK partition assignment is unbalanced.

**Investigation.**
- CloudWatch MSK dashboard: per-broker CPU, network, EBS IOPS.
- Per-topic partition skew: `aws kafka describe-cluster-operation` if a rebalance is in flight.

**Resolution.**
- Sustained: bump broker `instance_type` (default `kafka.t3.small`) via Terraform.
- Spike: tune consumer fetch sizes; ensure consumer groups are stable (avoid rapid pod restarts).

### Symptom: Iceberg table reads return stale data

**What it means.** A writer committed a new snapshot the reader hasn't refreshed against, or the Glue catalog isn't propagating.

**Investigation.**
- `SELECT * FROM "$table$snapshots"` via Trino — confirm latest snapshot id matches what the writer committed.
- AWS CloudTrail: confirm the writer's `UpdateTable` API call succeeded.

**Resolution.**
- Trino: `CALL system.invalidate_metadata_cache('catalog', 'schema', 'table')`.
- Writer: confirm the Java application's Iceberg client is on a recent enough version (snapshot-isolation bugs in older versions are documented).

### Cloud DR

[Disaster Recovery](#disaster-recovery) above applies. Cloud-specific additions:

- **S3 versioning** is enabled; recover accidentally-deleted Parquet via `aws s3api list-object-versions` + restore.
- **RDS automated snapshots** are retained per the Terraform default (7 days); restore via `aws rds restore-db-instance-from-db-snapshot`.
- **MSK has no point-in-time-restore.** Treat the Kafka log as ephemeral past its retention window; rely on the Parquet/Iceberg warehouse for long-term truth.

---

## Adding to This Runbook

After every operational incident, add or update a section:

- What was the symptom? (literal log line, metric name, alert)
- What was the cause? (one sentence)
- What were the steps to resolve?
- What would prevent a repeat?

Each addition makes the next operator's life easier — and that operator might be you, in three months, at 2 AM.
