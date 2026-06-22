# CLAUDE.md

## What Is Muninn

Muninn is an event-native research infrastructure platform built for deterministic replay, immutable event streams, reproducible feature computation, and real-time + historical parity. Named after Odin's raven of memory.

## Core Philosophy (Non-Negotiable)

- Events are **immutable, append-only facts** — the single source of truth
- All derived state (features, aggregations, views) must be **recomputable** from the event stream
- **Deterministic replay** — replaying the same events must produce identical results
- **Real-time/historical parity** — the same computation logic serves both live and replay paths
- No mutation of event data, no soft deletes, no in-place updates to the event log
- Batch correctness > streaming speed; simplicity > cleverness

## Commands

```bash
# Run all infrastructure locally
docker compose up -d

# Build the application
mvn clean package

# Run tests
mvn test

# Run a single test class
mvn test -Dtest=EventTest

# Run the application
mvn spring-boot:run

# Build Docker image
docker build -t muninn:latest .

# Cross-stack smoke test (all Norse services)
bash scripts/smoke-stack.sh --teardown
```

## Local Service Endpoints

| Service | URL |
|---------|-----|
| Muninn API | http://localhost:8080 |
| PostgreSQL | localhost:5433 |
| Redpanda (Kafka) | localhost:19092 |
| Redpanda Console | http://localhost:8088 |
| MinIO API | http://localhost:9002 |
| MinIO Console | http://localhost:9003 |
| Redpanda Schema Registry | http://localhost:18081 |

## Architecture

```
Event Producers → Ingestion API (HTTP) → Redpanda (Kafka)
                                              ↓
                                   Stream Processor (Consumer)
                                              ↓
                                   Parquet → MinIO (S3)
                                              ↓
                                   DuckDB (analytical queries)

PostgreSQL: event metadata, replay cursors, feature definitions
Redpanda: immutable event log, replay source
MinIO: Parquet file storage (raw + warehouse)
DuckDB: analytical query engine over Parquet
```

## Package Structure

- `event/` — Core event types (Event, EventEnvelope). Immutable records.
- `ingestion/` — HTTP API for event ingestion, Kafka producer.
- `replay/` — Deterministic replay engine. Seeks by timestamp, replays to handler.
- `storage/` — ObjectStore interface (MinIO), DuckDB query service, Parquet writer.
- `feature/` — Feature definitions and computation engine.
- `stream/` — Kafka consumer, stream processing pipeline.
- `config/` — Spring configuration (Kafka, S3, DuckDB).

## Key Types

- `Event` — Immutable event record: eventId (UUID), eventType, eventTime, sequenceNumber, source, partitionKey, payload
- `EventEnvelope` — Wraps an Event with Kafka metadata (topic, partition, offset, ingestedAt)
- `ReplayRequest` — Defines a replay window: topic, from, to (timestamps)
- `FeatureDefinition` — Declarative feature spec: source topic, aggregation, window, expression

## Tech Stack

- **Java 21**, **Spring Boot 4.1.0**, **Springdoc 3.0.3**
- **Redpanda** (Kafka-compatible), **PostgreSQL 16**, **MinIO** (S3-compatible), **DuckDB**
- **Iceberg** + **Trino** for production-reference archival/query path

## Norse Stack (Sibling Repos)

Muninn is the feature engine in a four-service pipeline:

```
Exchange → Muninn (features) → Huginn (strategy) → Sleipnir (execution) → Fill
```

- **[huginn](../huginn)** — Go strategy engine. Consumes `features.obi.v1`, produces `executions.intents.v1`.
- **[sleipnir](../sleipnir)** — Go execution gateway. Consumes intents, produces `executions.fills.v1`.
- **[muninn-py](../muninn-py)** — Python research SDK + CLI.
- Cross-stack test: `docker-compose.stack.yml` + `scripts/smoke-stack.sh`.

## Testing

Unit tests use JUnit 5. Integration tests use Testcontainers for PostgreSQL, Redpanda, and MinIO.

## Data Layout

```
MinIO:
  muninn-raw/        # Raw event Parquet files
  muninn-warehouse/  # Computed feature Parquet files

PostgreSQL:
  event_metadata     # Event index and Parquet file pointers
  replay_cursors     # Consumer group replay positions
  feature_definitions # Registered feature computations
```
