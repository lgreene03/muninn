# 0006. Trino-backed Query API for production-reference

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Project maintainer
- **Related:** [ADR-0005 — Iceberg with Glue catalog](0005-iceberg-with-glue-catalog.md), [DATA_STORAGE_STRATEGY.md](../steering/DATA_STORAGE_STRATEGY.md), [PHASE8_MIGRATION.md](../steering/PHASE8_MIGRATION.md), [DEPLOY.md](../DEPLOY.md)

## Context

Phase 8's production-reference profile replaces local DuckDB-over-Parquet with Trino-over-Iceberg (ADR-0005). The infrastructure side of that swap landed in `local-infra/terraform/aws/` and `deploy/helm/muninn/`. The application side did not: `FeatureQueryService` still held DuckDB-specific SQL inline (`read_parquet(...)` with Hive partitioning), so a `production-reference` deploy provisioned Trino but never used it.

Two options were available:

1. **One backend with dialect-aware SQL.** Keep `FeatureQueryService`, conditionally render DuckDB or Trino SQL based on a property.
2. **Pull the operation up to an interface.** Introduce a `FeatureQueryBackend` abstraction with `DuckDb*` and `Trino*` implementations; pick one at startup by `muninn.query.backend`.

Option 1 keeps the surface smaller in the short term but couples the service to every backend's quirks (DuckDB's `read_parquet()` table function is parameter-unfriendly; Trino's Iceberg tables don't have a Parquet path). The set of dialect-conditional branches would grow with every backend choice (Iceberg-specific SQL, time-travel queries, partitioned-by-version reads).

Option 2 isolates each backend's dialect in its own class, makes "what does the controller depend on?" answerable in one ArchUnit rule, and makes adding a third backend a matter of one new file plus a `@ConditionalOnProperty` branch.

## Decision

Introduce `io.muninn.query.backend.FeatureQueryBackend` as the abstraction the Query API delegates to. Provide two implementations:

- **`DuckDbFeatureQueryBackend`** — default; refactored from the existing DuckDB SQL, behavior unchanged.
- **`TrinoFeatureQueryBackend`** — new; uses `io.trino:trino-jdbc:434` against an Iceberg catalog.

Backend selection is property-driven:

```
muninn.query.backend = duckdb    # default — every local-* and cloud-cheap profile
muninn.query.backend = trino     # production-reference and any future cloud profile
```

`FeatureQueryBackendConfiguration` uses Spring Boot's `@ConditionalOnProperty` to wire exactly one. Only one branch's beans instantiate per app start, so the Trino JDBC connection pool never opens when DuckDB is active and vice versa.

The Query API surface (`FeatureQueryController`, `FeatureQueryService`) depends only on the abstraction. An ArchUnit rule (`query_api_depends_only_on_backend_abstraction` in `ArchitectureRulesTest`) enforces that the abstraction is the only path from controller to backend.

### Naming convention for Iceberg tables

The Trino backend reads from tables named `<catalog>.<schema>.features_<name>_<version>`, with dots and dashes in the feature name replaced by underscores. So `vwap.1m` (feature) at version `v1` becomes `iceberg.muninn.features_vwap_1m`. The transformation is implemented in `TrinoFeatureQueryBackend.tableNameFor(...)` and must be matched by whichever Iceberg writer registers tables on the writer side (the next streaming-data-engineer task).

### Driver-footprint footnote

`io.trino:trino-jdbc:434` adds ~18 MB to the runtime classpath after transitive pruning. The JAR remains comfortably under the 100 MB GitHub blob limit and within the local-first memory budget. If the footprint becomes a problem on a constrained edge profile (`cloud-cheap`), the driver moves to a Maven profile activated by `-Pquery-backend-trino`. For MVP we ship it unconditionally so a `production-reference` Helm upgrade is one values flag rather than a republish-the-image dance.

## Consequences

**Easier.**

- Adding a third backend (e.g., a hypothetical `IcebergJavaClientFeatureQueryBackend` that bypasses Trino) is one new class plus one bean definition. The controller doesn't change.
- `muninn.query.requests` and `muninn.query.latency` metrics carry a `backend` tag, so a mixed deployment (e.g., during migration) is observable per-engine in dashboards.
- DuckDB-specific behavior — including the "empty Parquet warehouse returns empty list, not 500" forgiveness — lives in `DuckDbFeatureQueryBackend` and is no longer mixed into the service layer.

**Harder / cost.**

- Two implementations mean two test surfaces. `DuckDbFeatureQueryBackendTest` and `TrinoFeatureQueryBackendTest` both exist; the Trino test currently mocks the JDBC surface rather than running against a real Trino + Iceberg + Glue, because spinning that triple up in CI is heavy. Tracked as a follow-up.
- The Trino JDBC driver dependency increases the JAR. Quantified above; acceptable.
- The Iceberg writer (separate task) must use the same `tableNameFor()` transformation, or the Trino backend won't find its tables. The contract is documented here so the writer's author can match it.

**Operational.**

- `production-reference` Helm values default to `query.backend: duckdb` for safety; switching to Trino is one values flag (`query.backend=trino`) plus the connection block (`query.trino.host`, `port`, `user`, `catalog`, `schema`, `ssl`). Documented in `docs/DEPLOY.md`.
- The migration sequence is: provision Trino + Iceberg + Glue; backfill Iceberg from Parquet via the (forthcoming) migration script; switch `query.backend` to `trino` per-instance; remove DuckDB from the rollout when both backends report identical results for a reference query window.

## Alternatives Considered

- **Dialect-rendering in a single service.** Rejected as described above.
- **Replace DuckDB outright.** Rejected — DuckDB stays the local-first default. The local-full and local-lite profiles can't run a Trino coordinator at the project's memory budget.
- **Native Iceberg Java client without Trino.** Considered. The Java Iceberg client is capable but at the project's MVP query complexity, Trino's `SELECT` ergonomics + the planner's predicate pushdown beat hand-writing scan-and-filter logic. Revisit if Trino becomes operationally heavy.
- **GraphQL or gRPC contract change.** Out of scope; this ADR is a storage-engine swap behind an unchanged HTTP contract.

## References

- [ADR-0005 — Iceberg with Glue catalog](0005-iceberg-with-glue-catalog.md) — the storage decision this backend serves queries against.
- `src/main/java/io/muninn/query/backend/FeatureQueryBackend.java` — the abstraction.
- `src/main/java/io/muninn/query/backend/FeatureQueryBackendConfiguration.java` — the wiring.
- `src/test/java/io/muninn/architecture/ArchitectureRulesTest.java::query_api_depends_only_on_backend_abstraction` — the enforced boundary.
- Trino JDBC driver — https://trino.io/docs/current/client/jdbc.html
