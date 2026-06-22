# TECH_STACK.md

This document records every technology choice in the MVP, why it was chosen, the constraints it satisfies, and the conditions under which it would be replaced. Choices are deliberate, not defaults.

## MVP Stack

### Java 21

**Why.** Mature, performant, excellent observability tooling, first-class records and pattern matching that suit event-modeling, and long-term support. Project Loom (virtual threads) lets a single process handle many concurrent event streams without an actor framework.

**When to replace.** Likely never. Possible jump to Java 25/LTS as it ships.

---

### Spring Boot 4.1

**Why.** Industry-standard application framework. Strong Kafka, JPA, and Actuator support. Production-grade observability via Micrometer baked in. Profiles map cleanly to the four deployment profiles (`local-lite`, `local-full`, `cloud-cheap`, `production-reference`).

**Trade-off.** Heavier than Quarkus or Helidon, but the productivity and ecosystem value outweigh the startup cost.

**When to replace.** If GraalVM native-image becomes a hard requirement, evaluate Quarkus or Spring Native. Not before.

---

### Redpanda (Kafka-compatible broker)

**Why.** Wire-compatible with Kafka, single-binary deployment, no ZooKeeper, low memory footprint. Suits the local-first constraint perfectly: a single container with 1 GB RAM serves the MVP throughput trivially.

**When to replace.** When the deployment target is a managed Kafka service (MSK, Confluent Cloud, Redpanda Cloud). The application code does not change — only the broker endpoint and credentials.

---

### DuckDB

**Why.** Embedded, single-file analytical database that reads Parquet directly from local disk or S3. Eliminates the need for a separate query engine in the MVP. Excellent for ad-hoc analysis, dashboards, and the `query-api` read path.

**Trade-off.** Single-node. Not designed for concurrent write-heavy workloads or multi-tenant analytics.

**When to replace.** When concurrent analytical queries exceed what a single embedded engine can serve, migrate to Trino reading the same Parquet (or Iceberg) data. No data migration required.

---

### Parquet

**Why.** Columnar, compressed, splittable, widely supported by every analytical tool that matters (DuckDB, Trino, Spark, Polars, Pandas). The canonical archival format for the event log after Redpanda retention rolls over.

**When to replace.** Augment with Apache Iceberg in Phase 8 for ACID semantics, time-travel queries, and schema evolution across many writers. Parquet remains the underlying file format.

---

### MinIO

**Why.** S3-compatible object storage that runs in a Docker container. Lets `production-reference` swap to actual S3 with no code change.

**When to replace.** In production, replace the container with AWS S3, GCS, or Backblaze B2. The S3 SDK code is unchanged.

---

### PostgreSQL 16

**Why.** Reliable, well-understood relational store for **metadata only**: feature definitions, replay-job status, instrument/exchange reference data, replay cursors. Not used for event storage.

**When to replace.** Likely never. In `cloud-cheap`, use a managed Postgres free tier (Supabase, Neon, Fly Postgres).

---

### Docker Compose

**Why.** The simplest way to orchestrate the local stack. Profiles via Compose overlays match Spring profiles.

**When to replace.** For `production-reference`, replace with Kubernetes + Helm. The Compose files document the architecture for translation; they remain canonical for local development indefinitely.

---

### Testcontainers

**Why.** Programmatic, hermetic integration testing against real Redpanda, real PostgreSQL, real MinIO. Eliminates the gap between unit tests and live behavior. Critical for replay-determinism tests.

**When to replace.** Never. This is the right tool.

---

### OpenTelemetry + Micrometer

**Why.** Vendor-neutral instrumentation. Micrometer for metrics (exposes Prometheus), OpenTelemetry for traces (exports to Tempo or Jaeger locally, any OTLP backend in cloud). Spring Boot 3 wires both natively.

**Trade-off.** Slightly higher overhead than custom-baked instrumentation. Not material.

**When to replace.** Never as a standard. Backends (Prometheus, Tempo, Loki) are swappable.

---

## Local Observability Stack

- **Prometheus** for metrics scrape.
- **Grafana** for dashboards.
- **Tempo** (or **Jaeger**, simpler) for traces.
- **Loki** for structured-log aggregation (optional in `local-lite`).

All run under the same Docker Compose profile (`local-full`).

## Build & Tooling

- **Maven** as the build tool. Familiar, well-supported, deterministic.
- **Spotless** for code formatting.
- **ArchUnit** for enforcing architectural rules (no `Instant.now()` in feature-engine packages, etc.).
- **JUnit 5**, **AssertJ**, **Mockito** for tests.

## Later Scalable Stack

When the MVP outgrows local-first, the migration path is well-defined:

| MVP component   | Scaled replacement              | Migration effort       |
|-----------------|---------------------------------|------------------------|
| Redpanda single | Kafka / Redpanda cluster        | Config only            |
| Parquet on MinIO| Apache Iceberg on S3            | Add Iceberg catalog    |
| DuckDB          | Trino                           | New query layer; same data |
| Spring Boot mono| Multi-process services          | Compose -> Helm        |
| Compose         | Kubernetes + Terraform          | New manifests          |
| Local Tempo     | Managed APM (Tempo Cloud, etc.) | Endpoint swap          |

The MVP is **scaled-down**, not **scaled-different**. Every choice anticipates its scaled equivalent.

## What We Deliberately Did Not Choose

- **Kafka Streams / Flink / Spark Structured Streaming.** Too heavy for MVP. The feature engine is a hand-written, deterministic processor — see [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md).
- **Cassandra / ScyllaDB / DynamoDB.** No need for a wide-column store in the MVP.
- **Redis.** Optional cache, not architecturally required. Add only if measured latency demands.
- **gRPC.** No synchronous RPC between event-producing services (see [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md)).
- **Kotlin.** No reason to deviate from Java; the team-onboarding cost is negative.
- **Reactor / WebFlux.** Loom virtual threads cover most use cases without callback complexity.
- **GraphQL.** REST is sufficient for the MVP query API.
