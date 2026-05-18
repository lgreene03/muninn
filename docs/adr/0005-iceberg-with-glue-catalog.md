# 0005. Apache Iceberg with AWS Glue as the catalog

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Project maintainer
- **Related:** [docs/steering/DATA_STORAGE_STRATEGY.md](../steering/DATA_STORAGE_STRATEGY.md), [docs/steering/EVENT_SCHEMA_STRATEGY.md](../steering/EVENT_SCHEMA_STRATEGY.md), [docs/steering/ROADMAP.md](../steering/ROADMAP.md) §Phase 8, `local-infra/terraform/aws/modules/s3_iceberg/`

## Context

Today Muninn writes Parquet files directly to MinIO (local) or S3 (cloud). DuckDB queries them with `httpfs`. This works for one writer (`FeatureArchivalConsumer`) and one reader (the Query API).

Phase 8 imagines:

- Multiple writers: the replay engine emitting to the same warehouse path the live archival writes; future exchange adapters; future feature versions.
- Multiple readers: Trino (replacing DuckDB at scale), Spark for ad-hoc, notebook libraries.
- Schema evolution that doesn't require rewriting files.
- Atomic appends, time-travel queries, partition evolution.

These are the problems [Apache Iceberg](https://iceberg.apache.org/) solves. The decision is which **catalog** to point Iceberg at — Iceberg the table format is the easy part; the catalog is where engines coordinate.

| Catalog | Pros | Cons |
|---|---|---|
| AWS Glue Data Catalog | Native AWS; supported by Trino, Spark, Flink, Athena, EMR; IAM-governed; per-region; no extra service to run | AWS-only; per-table cost (negligible at our scale) |
| Hive Metastore | Mature, widely supported | Requires a running RDBMS-backed Hive process; extra ops burden |
| Project Nessie | Git-style branching, multi-table transactions | New service to run; immature integration with some engines |
| REST catalog (self-hosted) | Vendor-neutral, lightweight | Requires running a process and an auth story; extra ops |

## Decision

Phase 8 adopts **Apache Iceberg** as the table format for the feature warehouse, with **AWS Glue Data Catalog** as the catalog. The Terraform module at [`local-infra/terraform/aws/modules/s3_iceberg/`](../../local-infra/terraform/aws/modules/s3_iceberg/) provisions both the S3 bucket and the Glue database.

The Parquet layout used today becomes the **physical layout** for Iceberg tables. The migration converts existing Parquet directories into Iceberg tables in place (rewriting only the metadata, not the data files). See [PHASE8_MIGRATION.md](../steering/PHASE8_MIGRATION.md).

Trino reads via the Iceberg connector with `iceberg.catalog.type=glue`. The Java application uses the Iceberg Java library when it migrates from raw Parquet writes (Phase 8 application work, not yet in `main`).

## Rationale

- **Iceberg solves real problems Muninn will hit.** Concurrent writers (replay vs live) need atomic appends. Schema evolution needs a contract. Time-travel queries answer "what did the warehouse look like at the moment of yesterday's audit?".
- **Glue is the lowest-friction catalog in AWS.** Trino, Spark, Athena, EMR all speak to it natively. The Terraform module is one resource (`aws_glue_catalog_database`).
- **Glue is also the cheapest answer.** Per-table cost rounds to zero at this scale.
- **Parquet stays.** Iceberg sits on top of Parquet files. The migration is mechanical, not destructive.

## Consequences

**Easier.** Once Iceberg is in place, replay outputs can land in the same table the live engine writes to without coordination — Iceberg's snapshot isolation handles it. Schema changes (a new nullable field on `FeatureComputedEvent`) propagate through one `ALTER TABLE` statement, not a file rewrite. Time-travel queries (`AS OF '2026-05-10T14:00:00Z'`) answer audit questions that today require re-running the replay engine.

**Harder.** Operators have one more concept to learn (Iceberg snapshots, manifests, compaction). The application code needs an Iceberg writer (Java library) — that work is Phase 8 application-side and **not yet in `main`**. Local development picks up a `local-full` profile dependency: either run Iceberg against MinIO (works), or keep raw Parquet locally and switch only in cloud (also works).

**Cost.** Glue catalog operations are billed per request — at our cadence (one append per minute per feature), well under any reasonable budget. S3 storage is unchanged.

## Alternatives Considered

- **Stay on raw Parquet.** Rejected for Phase 8 because the concurrent-writer story (replay + live writing to the same partition family) is not solvable without atomic appends.
- **Delta Lake.** Considered. Comparable feature set to Iceberg. Rejected because Iceberg has better Trino/Flink support in the open-source space and a less Databricks-aligned governance model.
- **Hudi.** Considered. Strong for upserts; Muninn's workload is append-only, so Hudi's strengths don't apply.
- **Hive Metastore.** Rejected for the operational cost.
- **Project Nessie.** Documented as a follow-up option if branching semantics become useful for "what-if" research.

## References

- [Apache Iceberg](https://iceberg.apache.org/)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/components-overview.html)
- [`local-infra/terraform/aws/modules/s3_iceberg/`](../../local-infra/terraform/aws/modules/s3_iceberg/)
- [PHASE8_MIGRATION.md](../steering/PHASE8_MIGRATION.md) — migration playbook.
