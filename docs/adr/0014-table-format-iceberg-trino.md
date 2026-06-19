# 0014. Table Format: Apache Iceberg & Trino

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Antigravity (AI Orchestrator), lgreene03 (Operator)
- **Related:** [docs/steering/DATA_STORAGE_STRATEGY.md](../steering/DATA_STORAGE_STRATEGY.md), [ADR-0012](0012-duckdb-query-shape.md)

## Context

In our local-first MVP profile, `query-api` processes analytical queries directly over cold Parquet files using an embedded DuckDB JDBC query runner. While DuckDB is exceptionally fast for single-process analytical execution on a single host, it suffers from several core limitations at production scale:
1. **Concurrency Bottlenecks:** Embedded DuckDB cannot easily handle highly concurrent analytical queries from dozens of active client laptops or notebooks.
2. **ACID Lackings:** Standard Parquet directories lack transactional safety guarantees when concurrent writers are committing new rolling windows or backtesting feature replays.
3. **No Schema Evolution:** As feature engineering rules evolve, modifying schema fields across ancient Parquet files without full rewrites is extremely difficult.

## Decision

We accept Apache Iceberg as our historical table storage format and Trino as our distributed analytical query engine. 

Iceberg sits on top of our existing S3 Parquet files, organizing them with metadata tables to provide ACID transactions, time-travel, and schema evolution. The `query-api` module's JDBC connection is designed to swap its DuckDB connection (`jdbc:duckdb:...`) for a Trino distributed query coordinator connection (`jdbc:trino:...`) using standard HikariCP datasource configurations in our cloud profiles.

## Consequences

*   **What becomes easier:**
    *   **ACID Transactions:** Writers can commit rolling partitions, and readers can execute backtests simultaneously with zero read corruption or lockouts.
    *   **Schema Evolution:** Add, rename, or drop feature output fields inside the Iceberg metadata catalog without rewriting existing underlying Parquet data.
    *   **High Concurrency:** Trino distributes queries across an EKS worker node cluster, easily scaling to dozens of concurrent research sessions.
*   **What becomes harder:**
    *   **Metastore Management:** Maintaining an external metadata catalog (e.g. AWS Glue or JDBC-backed metastore) adds a database layer to track table commits.
    *   **Setup Complexity:** Running distributed Trino coordinators and workers in Kubernetes requires careful memory limit allocation and configuration.
*   **Consequences & Follow-up:**
    *   We introduce PyIceberg-based migration scripts (`migrate_parquet_to_iceberg.py`) to map existing Parquet files into the Iceberg catalog *in-place*.
    *   Trino compose overlays (`docker-compose.trino.yml`) are added to allow local simulation of Trino-over-Iceberg queries.

## Alternatives Considered

*   **Delta Lake.** Rejected due to heavier dependency on Spark ecosystem pipelines, whereas PyIceberg and Trino offer lightweight, native Python/SQL interactions.
*   **Status quo (Embedded DuckDB).** Rejected due to strict single-process write-lock limits and lack of native distributed clustering for concurrent users.

## References

- [Apache Iceberg Table Format Specification](https://iceberg.apache.org/spec/)
- [Trino Distributed SQL Query Engine](https://trino.io/docs/current/)
