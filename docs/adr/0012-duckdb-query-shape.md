# ADR-0012: DuckDB as the Query Engine for Parquet over MinIO

**Status:** Accepted  
**Date:** 2026-05-18  
**Deciders:** Muninn core team

---

## Context

Phase 5 requires a read-only HTTP API that serves feature time-series data for arbitrary historical time ranges. Feature data is stored as Parquet files in MinIO, partitioned by `features.<name>.<version>/<year>/<month>/<day>/`.

The system must remain local-first: no separate query process, no managed cloud service, no Trino cluster for the MVP.

---

## Decision

Use **DuckDB** as the embedded analytical query engine, with the `httpfs` extension to read Parquet files directly from MinIO (S3-compatible).

The concrete implementation:
1. `DuckDbConnectionManager` loads `httpfs` on every connection and configures S3 session variables (`s3_endpoint`, `s3_access_key_id`, `s3_secret_access_key`, `s3_region`, `s3_use_ssl`, `s3_url_style=path`) from the existing `StorageConfig.S3Properties` bean.
2. `FeatureQueryService` constructs SQL using `read_parquet('s3://muninn-warehouse/...')` with `hive_partitioning=true` and passes it to `DuckDbQueryService.query(sql)`.
3. No DuckDB state is persisted. DuckDB is a stateless query engine over the Parquet files.

---

## Consequences

**Positive:**
- No new process to operate. DuckDB is embedded in the JVM process.
- Parquet + `httpfs` gives full SQL over S3 without ETL.
- The data layout is unchanged from what the Parquet writer already produces.
- Migration to Trino (Phase 8) requires only swapping the query executor, not the storage layout.

**Negative:**
- DuckDB is single-threaded per connection in the MVP. Concurrent heavy analytical queries are not supported at this stage.
- The `httpfs` extension is loaded on every connection, adding ~10ms overhead per request. Acceptable for MVP latency budget.

---

## Alternatives Considered

| Option | Reason rejected |
|:---|:---|
| **Trino** | Requires a separate cluster. Non-starter for local-first MVP. |
| **Direct S3 SDK reads** | No SQL, no predicate pushdown, excessive application code. |
| **Apache Spark** | Too heavy; startup latency incompatible with request/response API. |
| **Persist data to DuckDB file** | Creates a derived store that can diverge from Parquet truth. Violates event-native principle. |
