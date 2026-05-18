# DATA_STORAGE_STRATEGY.md

Muninn stores data in four places. Each has a precise purpose. Confusing them is how systems become unrecoverable.

## Storage Layers at a Glance

```
                    +------------------+
   producers ---->  |   Event log      |  Redpanda (hot, last 7 days)
                    +--------+---------+
                             |
                             | retention rollover
                             v
                    +------------------+
                    |  Parquet files   |  MinIO  (warm, 90 days)
                    +--------+---------+
                             |
                             | queried by
                             v
                    +------------------+
                    |     DuckDB       |  embedded query engine
                    +------------------+

                    +------------------+
                    |   PostgreSQL     |  metadata only (not events)
                    +------------------+

                    +------------------+
                    |     Redis        |  optional cache (Phase 5+)
                    +------------------+
```

---

## 1. Event Log — Redpanda

**Role.** The canonical, immutable, append-only record of every fact the system has observed. The single source of truth.

**Contents.**
- `events.trade` — `TradeEvent`s.
- `events.book.snapshot` — `OrderBookSnapshotEvent`s.
- `events.candle` — exchange-reported `CandleEvent`s (when available).
- `features.<name>.v<n>` — `FeatureComputedEvent`s, partitioned by feature name and version.
- `events.deadletter` — rejected events with structured failure reasons.

**Retention.** 7 days locally. The rollover process is responsible for archiving older segments to Parquet in MinIO.

**Partitioning.** By `(source, instrument)`. Within a partition, events are strictly ordered by `eventTime` (enforced at ingestion).

**Properties.**
- Idempotent producers (`enable.idempotence=true`, `acks=all`).
- Compacted topics are **never** used for raw event topics. Compaction breaks replay.
- Time-indexed: every segment has a timestamp index for seek-by-timestamp replay.

---

## 2. Parquet Files — MinIO

**Role.** Warm archival storage for events older than the Redpanda retention window. The format from which long-range replay reads.

**Layout.**
```
muninn-raw/
  events.trade/
    source=coinbase/instrument=BTC-USD/
      year=2026/month=05/day=11/hour=14/
        part-00000-<uuid>.parquet
  events.book.snapshot/
    ...

muninn-warehouse/
  features.<name>.v<n>/
    year=.../month=.../day=.../
      part-<uuid>.parquet
```

**Partitioning.** Hive-style by `source`, `instrument`, `year`, `month`, `day`, `hour`. This makes DuckDB and (later) Trino pruning trivial.

**Compaction.** A daily job compacts small files into ≥ 128 MB Parquet files per partition.

**Properties.**
- Files are immutable. New data appears as new files; existing files are never rewritten in place. Compaction writes new files and atomically deletes the old.
- Schema is recorded in the Parquet metadata; the canonical Java record is the writer.

---

## 3. Query Layer — DuckDB

**Role.** Analytical query engine. DuckDB reads Parquet directly from MinIO (via the `httpfs` extension) or from local disk in `local-lite`.

**Use cases.**
- The `query-api` answers feature time-series requests.
- Ad-hoc notebook queries against the warehouse.
- Golden-dataset replay validation.

**Properties.**
- Single-node, embedded. No separate process to operate.
- No persistent DuckDB state — it is a query engine over the Parquet files. Restarts cost nothing.
- Query timeouts and memory limits configured per profile.

**When DuckDB is not enough.** When concurrent analytical queries exceed what a single embedded engine can serve, swap in Trino. The data layout is identical; only the query engine changes.

**Switching backends.** The Query API delegates feature reads to a `FeatureQueryBackend` abstraction with two implementations: `DuckDbFeatureQueryBackend` (default, reads Parquet directly) and `TrinoFeatureQueryBackend` (production-reference, reads Iceberg tables via JDBC). Selection is property-driven via `muninn.query.backend` (`duckdb` | `trino`). See [ADR-0006](../adr/0006-trino-query-backend.md) for the rationale and the Iceberg table-naming convention the Trino backend expects.

---

## 4. Metadata Store — PostgreSQL

**Role.** Small, transactional metadata. **Never** event data.

**Tables.**
- `instruments` — symbol, base/quote asset, exchange.
- `exchanges` — id, name, timezone.
- `feature_definitions` — registered feature specs.
- `feature_versions` — version → code SHA → activation timestamp.
- `replay_jobs` — job state, ranges, outputs.
- `replay_cursors` — consumer-group offsets per topic per partition.
- `event_index` (optional) — pointer from event-id to Parquet file + row group; used for cross-cutting forensic queries.

**Properties.**
- Standard transactional database.
- Migrations via Flyway. Backward-compatible only.
- Small enough that a free-tier managed Postgres covers the `cloud-cheap` profile.

**Why not events here.** PostgreSQL is wrong for an immutable event log of millions to billions of rows. Use the broker for hot, Parquet for cold.

---

## 5. Optional Cache — Redis

**Role.** Optional read-side cache for the `query-api`. **Not** required.

**Added when.** Measured `query-api` latency exceeds the SLA (e.g., > 100 ms p95 for a feature-time-series query) and DuckDB tuning is exhausted.

**Properties.**
- Cache only. Never authoritative.
- Bounded memory.
- TTLs aligned with Parquet partition granularity (e.g., 1 hour for hourly partitions).

If the MVP doesn't need Redis, don't ship Redis.

---

## Later: Apache Iceberg + Trino

**When.** Phase 8 (`production-reference`).

**Why.** Iceberg gives ACID over Parquet, time-travel queries, schema evolution across many writers, and a catalog that Trino, Spark, and Flink can share. Trino gives multi-tenant, concurrent analytical queries at scale.

**Migration mechanism.** Iceberg sits on top of the existing Parquet files. The catalog is the new piece; the data is unchanged. The `query-api` swaps DuckDB for a Trino client. Code that writes Parquet today swaps for an Iceberg writer with the same Java record.

This migration is anticipated by the MVP architecture. It is **not** built in MVP.

---

## Retention and Deletion

- **Events are never deleted manually.** Retention is by policy: Redpanda topic retention, then Parquet file age.
- **Local-first retention defaults**: 7 days hot, 90 days warm, configurable per profile.
- **Deletion is by partition prune**, not by row. Removing data means removing whole Parquet partition directories.
- **GDPR-style erasure** is out of scope for the MVP. The architecture does not preclude it; it is documented as a Phase 8+ concern.

## Disaster Recovery

- **Redpanda topics** are replicated within the broker (single-node in MVP, multi-node in production-reference).
- **MinIO Parquet** is backed up to a second object store (a second MinIO instance locally; a cold-storage bucket in production-reference).
- **PostgreSQL** is backed up via `pg_dump` nightly in `local-full`, managed snapshots in `cloud-cheap`.

The fundamental recovery property: as long as the event log is intact, every derived view can be rebuilt by replay.
