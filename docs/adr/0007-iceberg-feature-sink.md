# 0007. Iceberg-backed archival sink for production-reference

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Project maintainer
- **Related:** [ADR-0005 — Iceberg with Glue catalog](0005-iceberg-with-glue-catalog.md), [ADR-0006 — Trino-backed Query API](0006-trino-query-backend.md), [DATA_STORAGE_STRATEGY.md](../steering/DATA_STORAGE_STRATEGY.md), [DEPLOY.md](../DEPLOY.md)

## Context

ADR-0005 picked Iceberg + Glue as the production-reference warehouse format. ADR-0006 wired the Query API to read it via Trino. The remaining gap was on the writer side: `FeatureArchivalConsumer` flushed batches to raw Parquet files via `FeatureParquetWriter`, so a `production-reference` cluster would provision Iceberg + Glue and never populate them. The `scripts/migrate-parquet-to-iceberg.sh` backfill helper exists, but a steady-state production deploy needs writes to land in Iceberg directly.

Two approaches were available:

1. **Switch the writer outright.** Replace `FeatureParquetWriter` with an Iceberg implementation. Smaller code surface but couples local dev to the Iceberg dependency footprint and removes the simple "Parquet to MinIO" path that smoke tests exercise.
2. **Pull the operation up to an interface.** Mirror the `FeatureQueryBackend` pattern from ADR-0006 — introduce `FeatureSink` with `Parquet*` and `Iceberg*` implementations, profile-switched at startup.

The second option preserves the existing local-first path, keeps the Iceberg dependency tree only in `production-reference` runtime, and treats this writer swap symmetrically with the reader swap from ADR-0006.

## Decision

Introduce `io.muninn.storage.sink.FeatureSink` as the abstraction the archival consumer delegates to. Provide two implementations:

- **`ParquetFeatureSink`** — wraps the existing `FeatureParquetWriter` unchanged. Default for every `local-*` profile and `cloud-cheap`.
- **`IcebergFeatureSink`** — appends rows to Iceberg tables via the Iceberg Java API. Uses the AWS Glue catalog in `production-reference` (matching ADR-0005); supports a `hadoop` catalog as an escape hatch for non-AWS deployments and integration tests.

Backend selection is property-driven:

```
muninn.archival.sink = parquet    # default — every local-* and cloud-cheap profile
muninn.archival.sink = iceberg    # production-reference and any future cloud profile
```

`FeatureSinkConfiguration` uses `@ConditionalOnProperty` so exactly one sink instantiates per app start; the Iceberg catalog client never opens when Parquet is active and vice versa.

The archival consumer (`FeatureArchivalConsumer`) depends only on the abstraction. An ArchUnit rule (`archival_consumer_depends_only_on_sink_abstraction` in `ArchitectureRulesTest`) enforces that — only the configuration class sees concrete sinks.

### Table-naming contract

`IcebergFeatureSink.tableIdentifierFor("vwap.1m", "v1")` produces `<schema>.features_vwap_1m`, matching `TrinoFeatureQueryBackend.tableNameFor(...)` from ADR-0006. Both transformations lowercase the feature name and replace `.` and `-` with `_`. Without this match, the Trino read path can't find what this sink writes.

`featureVersion` is **not** part of the table name. It lives as a column on every row, and the query backend filters on it when version-specific results are required. This keeps successive versions of a feature in the same table — Iceberg's snapshot history covers the audit trail.

### Schema

Fixed for the MVP feature shape, matching the Avro schema used by `FeatureParquetWriter` so the `scripts/migrate-parquet-to-iceberg.sh` backfill can convert existing data without column mapping:

| Column | Iceberg type | Notes |
|---|---|---|
| `event_id` | `string` | UUIDv7 stringified |
| `event_time` | `timestamptz` | trigger event time |
| `window_start` | `timestamptz` | inclusive |
| `window_end` | `timestamptz` | exclusive |
| `instrument` | `string` | partition column |
| `value` | `string` | `BigDecimal.toPlainString()` — see ADR-0002 on numeric provenance |
| `input_event_count` | `long` | provenance count, not the IDs themselves at this level |
| `feature_name` | `string` | denormalized for filterability |
| `feature_version` | `string` | denormalized for filterability — see naming contract above |
| `code_version` | `string` | git SHA at compute time |

Partitioned by `instrument` (identity) and `window_start` (hour truncation) — same coarseness as the Hive partition layout the Parquet sink writes, so Trino's plan-pruning behaves consistently across backends.

### Catalog choices

| Value | Use |
|---|---|
| `glue` (default) | Production-reference. Matches ADR-0005. |
| `hadoop` | Filesystem-based catalog for tests and non-AWS deployments. Useful for the planned Testcontainers MinIO + HadoopCatalog integration test. |

A future entry could add `nessie` or `rest` once a real need surfaces.

### Dependency footprint

`org.apache.iceberg` core + data + parquet + aws together add roughly 6–8 MB to the classpath. The AWS SDK is already present (used by the existing S3Client), so `iceberg-aws` doesn't double-pull it. JAR remains comfortably below the 100 MB GitHub blob limit and well within JVM heap budgets in production-reference deployments.

## Consequences

**Easier.**

- The migration story is symmetric with ADR-0006: one Helm values flag flips the read path (`query.backend=trino`); a second flips the write path (`archival.sink=iceberg`). Production-reference rollout can stage them independently.
- The `muninn.archival.*` metrics (planned, surfaced in OBSERVABILITY_STRATEGY.md) will carry a `sink` tag like the query metrics carry `backend`, so dashboards can show per-sink rates during a migration.
- Adding a third sink (an in-memory test sink, for example) is one class + one bean definition. The archival consumer doesn't change.

**Harder / cost.**

- Two implementations means two test surfaces. `ParquetFeatureSinkTest` and `IcebergFeatureSinkTest` both exist; the Iceberg test currently exercises only the naming contract and input validation. End-to-end Parquet-append + snapshot-commit is tracked for a follow-up Testcontainers integration test (MinIO + HadoopCatalog) — heavy enough that doing it here would have ballooned this commit.
- The Iceberg dependencies add ~6–8 MB to the JAR even when `archival.sink=parquet`. Acceptable; an opt-in Maven profile is available if a future constrained edge profile needs it (parallel to the Trino driver footprint discussion in ADR-0006).
- The fixed schema is shared by both sinks for migration symmetry. Future schema evolution must be done in lock-step with the migration script and the Trino read path.

**Operational.**

- `production-reference` Helm values default to `archival.sink: parquet` for safety. Switching to Iceberg is a single values flag plus the catalog config block (`archival.iceberg.catalog-type`, `warehouse`, `glue-database`, `aws-region`, `schema`). Documented in `docs/DEPLOY.md`.
- Recommended migration sequence: enable Iceberg writes alongside Parquet (dual-write would require code, so for now: run the migration script to backfill existing Parquet → Iceberg, then switch `archival.sink=iceberg` per-instance, then switch `query.backend=trino`). Drain the Parquet path after Trino reports identical results against a reference query window.

## Alternatives Considered

- **Replace `FeatureParquetWriter` outright.** Rejected as above — keeps local profile dependency-light.
- **Iceberg-only with a `LocalCatalog` for local profiles.** Considered. The Iceberg HadoopCatalog can serve this role, but Parquet-to-MinIO without an Iceberg catalog is meaningfully simpler for the local-first promise. Keep Parquet as the local path; HadoopCatalog stays available as an Iceberg-on-local escape hatch.
- **Native Iceberg JDBC writer via Trino.** Trino does support `INSERT INTO ... SELECT ...` against Iceberg tables, but submitting a JDBC INSERT per batch from a streaming consumer adds coordinator load with no benefit. The Iceberg Java API is the right interface for direct writes.

## References

- ADR-0005 — the storage decision this sink serves.
- ADR-0006 — the symmetric read-side decision; the table-naming contract is shared between them.
- `src/main/java/io/muninn/storage/sink/FeatureSink.java` — the abstraction.
- `src/main/java/io/muninn/storage/sink/FeatureSinkConfiguration.java` — the wiring.
- `src/test/java/io/muninn/architecture/ArchitectureRulesTest.java::archival_consumer_depends_only_on_sink_abstraction` — the enforced boundary.
- Apache Iceberg Java API — https://iceberg.apache.org/docs/latest/java-api-quickstart/
