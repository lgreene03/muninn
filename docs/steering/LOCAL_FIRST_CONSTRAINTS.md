# LOCAL_FIRST_CONSTRAINTS.md

Muninn is local-first. The entire system must run, end to end, on a single developer machine without cloud services. This document defines the hard constraints that enforce that property.

## Reference Hardware

- **Mac mini M4**
- **24 GB unified memory**
- **SSD (≥ 256 GB free)**
- **macOS with Docker Desktop or OrbStack**

All MVP work must be developable, runnable, and testable on this machine. If a feature requires more than this, it does not belong in the MVP.

## Memory Budget

Total resident memory across all containers must stay under **12 GB** so the developer can still run an IDE, browser, and other tools.

Initial allocation:

| Component         | Memory cap  |
|-------------------|-------------|
| Redpanda          | 1 GB        |
| PostgreSQL        | 512 MB      |
| MinIO             | 512 MB      |
| ingestion-service | 1 GB        |
| feature-engine    | 2 GB        |
| replay-engine     | 2 GB        |
| query-api         | 1 GB        |
| Observability     | 1 GB        |
| **Reserved**      | **2 GB**    |
| **Total**         | **≈ 11 GB** |

Every service declares its memory cap in `docker-compose.yml`. Exceeding it is a regression.

## Data Retention

Local disks are not infinite. Retention caps are enforced by background compaction:

- **Raw event log (Redpanda)**: 7 days, then offloaded to Parquet in MinIO.
- **Parquet warehouse (MinIO)**: 90 days locally.
- **DuckDB working set**: ephemeral, bounded by query.
- **PostgreSQL metadata**: indefinite (small).

Retention is configurable per profile.

## Scope Constraints (MVP)

- **One exchange adapter** initially (e.g., Coinbase or Binance public market data).
- **One or two instruments** (e.g., `BTC-USD`, `ETH-USD`) — enough to exercise the system, not enough to overwhelm it.
- **One event type at a time** through the pipeline before adding the next.
- **No multi-tenant** considerations. Single deployment, single user.

These are not permanent limits; they are the bar for "the system works." Expansion is a Phase 8 concern.

## Forbidden in MVP

- Managed cloud services (AWS RDS, MSK, S3, BigQuery, Snowflake, Confluent Cloud).
- Kubernetes, Helm, k3s, k0s, microK8s.
- Distributed stream-processing engines (Flink, Spark, Kafka Streams cluster mode).
- Distributed query engines (Trino, Presto, Athena).
- Paid SaaS observability (Datadog, New Relic, Honeycomb).
- Anything that requires a credit card.

## Deployment Profiles

The same codebase must run under four profiles, controlled by Spring profile + Compose overlay:

### `local-lite`

The absolute minimum to exercise the pipeline. Redpanda, PostgreSQL, MinIO, and a single Spring Boot process running all modules. Suitable for laptops, CI, and the first 30 minutes after `git clone`.

### `local-full`

The full local stack: separate processes for ingestion, feature engine, replay engine, query API. Observability stack (Prometheus, Grafana, Tempo or Jaeger). Suitable for end-to-end development on the Mac mini M4.

### `cloud-cheap`

A deployable profile targeting free or near-free tiers: a single VPS, managed Postgres free tier, MinIO or B2 for object storage, Redpanda Cloud Developer tier. Suitable for a public demo.

### `production-reference`

The aspirational scaled-up profile: Kafka, Iceberg, Trino, Kubernetes, Terraform. This is documented but not built in the MVP — it is the proof that the architecture scales without a rewrite.

## Boot-Time Promise

`docker-compose up -d && ./scripts/smoke.sh` must complete successfully on the reference hardware within **5 minutes from a cold start, 90 seconds from a warm start**. Anything slower is a regression.

## Failure Modes

The system must survive:

- Sudden `docker-compose down` (no committed data loss).
- A 30-second broker outage (consumers reconnect, no events dropped).
- Restart of any single service (idempotent recovery, no manual intervention).

These are tested under [TESTING_STRATEGY.md](TESTING_STRATEGY.md).
