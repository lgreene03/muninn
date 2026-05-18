# PHASE8_MIGRATION.md

Operational playbook for moving a running Muninn deployment from the **`local-full`** profile (Redpanda + MinIO + Parquet + PostgreSQL + DuckDB on Docker Compose) to the **`production-reference`** profile (MSK + S3 + Iceberg + RDS + Trino on EKS).

This doc is the *what-changes-when* and the cutover order. The architectural rationale lives in [ADR-0003](../adr/0003-managed-kafka-via-msk.md), [ADR-0004](../adr/0004-eks-over-fargate-only.md), and [ADR-0005](../adr/0005-iceberg-with-glue-catalog.md).

## Mapping at a glance

| Local component | Cloud equivalent | Migration style |
|---|---|---|
| Redpanda (single-node) | Amazon MSK (3 brokers, TLS) | Dual-write cutover |
| MinIO | S3 | DNS / endpoint swap |
| Parquet (raw) | Iceberg on the same S3 paths | In-place metadata rewrite |
| PostgreSQL (container) | RDS for PostgreSQL | `pg_dump` → restore |
| DuckDB (in-process) | Trino on EKS | Query-API code swap (Phase 8 app work — see *Deferred* below) |
| Docker Compose | Kubernetes (EKS) via Helm | New deploy target |

## Pre-flight

```bash
# 0. The local stack runs cleanly and CI is green on main.
docker compose up -d --wait && ./scripts/smoke.sh
git log -1 --oneline

# 1. Provision the cloud infrastructure.
cd local-infra/terraform/aws
terraform init
terraform plan -var environment=staging
terraform apply -var environment=staging

# 2. Capture the outputs your application needs.
terraform output -json > ../../../target/aws-outputs.json
```

The outputs surface the MSK bootstrap endpoint, the S3 warehouse bucket, the Glue catalog database name, the RDS endpoint, and the EKS cluster name. The Helm `values.yaml` reads these via `--set` overrides during deployment.

## Step 1 — RDS for PostgreSQL (metadata only)

Metadata is small and well-defined; do this first.

```bash
# Dump the local metadata database.
docker compose exec postgres pg_dump -U muninn muninn > target/muninn-metadata.sql

# Restore into RDS.
psql "$(jq -r '.rds_endpoint.value' target/aws-outputs.json)" -U muninn -f target/muninn-metadata.sql

# Confirm migrations are aligned.
psql "$(...)" -U muninn -c "select * from flyway_schema_history order by installed_rank desc limit 5"
```

Flyway runs read-only by default in cloud profile; the schema came over with the dump. If the local schema has run a migration not yet committed to `main`, that's a real bug — fix it before proceeding.

## Step 2 — S3 warehouse seeding

```bash
# Mirror the existing Parquet warehouse to S3.
WAREHOUSE_LOCAL="./data/warehouse"
WAREHOUSE_S3=$(jq -r '.warehouse_bucket.value' target/aws-outputs.json)

aws s3 sync "$WAREHOUSE_LOCAL" "s3://${WAREHOUSE_S3}/" \
    --exclude "*.tmp" --exclude "*.crc"

# Verify a few partition counts match.
aws s3 ls "s3://${WAREHOUSE_S3}/features.vwap.1m.v1/" --recursive | wc -l
```

If the local warehouse is large, run this during a quiet window and pin the sync to a starting `mtime` so the final delta is small.

## Step 3 — Parquet → Iceberg (metadata-only rewrite)

The Parquet files stay where they are; we register them as Iceberg tables.

```bash
# Submit the conversion job via the bundled script (uses pyiceberg under the hood).
./scripts/migrate-parquet-to-iceberg.sh \
    --bucket "$WAREHOUSE_S3" \
    --catalog "$(jq -r '.glue_catalog_database.value' target/aws-outputs.json)" \
    --tables "features.vwap.1m.v1,events.trade,events.book.snapshot"

# Confirm tables are visible in Glue.
aws glue get-tables --database-name "muninn_staging_catalog" \
    --query 'TableList[].Name'
```

The script is idempotent — re-runs against an already-converted bucket are no-ops.

**Note.** The application-side Iceberg writer (Java) is Phase 8 application work and is **not yet on `main`**. Until it lands, the staging cloud deployment continues to write raw Parquet; Iceberg is read-only via Trino. The migration script supports that interim state.

## Step 4 — MSK cutover

This is the most sensitive step. Strategy: **dual-write from the application** during a bounded cutover window, then drain Redpanda and decommission.

### 4a. Create topics on MSK

```bash
MSK_BOOTSTRAP=$(jq -r '.msk_bootstrap_brokers_tls.value' target/aws-outputs.json)
KAFKA_BOOTSTRAP_SERVERS="$MSK_BOOTSTRAP" KAFKA_TLS=1 \
    ./scripts/create-topics.sh
```

The script tolerates TLS via `KAFKA_TLS=1` and uses `rpk` with the bundled CA chain.

### 4b. Configure the application to dual-write

Set both bootstrap servers in the Helm values:

```yaml
global:
  redpanda:
    bootstrapServers: "redpanda.svc.cluster.local:9092"
  msk:
    bootstrapServers: "<MSK_BOOTSTRAP_TLS>"
    enabled: true
muninn:
  ingestion:
    dualWrite: true              # produce to both; reads still come from Redpanda
```

Deploy this configuration. Watch `muninn.ingest.events.total{destination=...}` to confirm both sinks receive events.

### 4c. Switch consumers to MSK

Once dual-write has been running for at least one watermark window (default 1 minute) plus the worst-case consumer lag tolerance:

```yaml
muninn:
  features:
    engine:
      source: msk                # consumers move to MSK
  replay:
    source: msk
```

Verify `muninn.feature.events.processed` and `muninn.replay.divergence.detected` stay healthy. The divergence detector is the load-bearing safety check: if MSK and Redpanda inputs differ, divergence count goes up. Investigate any non-zero count before continuing.

### 4d. Stop the Redpanda producer

```yaml
muninn:
  ingestion:
    dualWrite: false             # produce to MSK only
```

Drain Redpanda topics (let consumers catch up to the latest offset), then tear down the Redpanda Deployment in Helm.

## Step 5 — Decommission local services

```bash
# Local Docker Compose stops; production deployment runs on EKS via Helm.
docker compose down
```

`docker compose` remains the recommended local-dev tool. The migration only retires the local stack from the production-deployment path.

## Validation

After each step, run the appropriate smoke check:

| Step | Validation |
|---|---|
| RDS | `flyway info` reports the same baseline as local |
| S3 | Object count matches local Parquet directory count ± in-flight |
| Iceberg | `SELECT count(*) FROM features.vwap.1m.v1` via Trino matches DuckDB count |
| MSK dual-write | Both topic counters increment at the same rate (Prometheus) |
| Consumer cutover | `muninn.replay.divergence.detected == 0` for a full nightly audit window |
| Final | `./scripts/smoke.sh --target cloud` end-to-end |

The `cloud` smoke target is documented in [DEPLOY.md](../DEPLOY.md).

## Rollback

Any step is reversible until **Step 4d** (stopping the Redpanda producer). Until then:

- Disable MSK dual-write in Helm; the application returns to Redpanda-only operation.
- Iceberg metadata can be deleted from Glue without touching the Parquet files.
- RDS metadata is read-only; the local PostgreSQL container remains authoritative until Step 1 is confirmed.

After Step 4d, rollback requires re-producing to Redpanda from MSK consumers — a one-time mirror, scriptable via `kafka-mirror-maker-2`.

## Deferred — Phase 8 application work

These are application-side changes scoped for Phase 8 but not yet on `main`:

- **Trino-backed Query API.** The query path needs a Trino JDBC client alongside the DuckDB client, with a `muninn.query.backend` profile property selecting between them. Dispatch: `backend-engineer`.
- **Iceberg writer in `feature-engine` and `replay-engine`.** Today the application writes raw Parquet via `FeatureParquetWriter`. The cloud profile needs an Iceberg-aware writer that registers snapshots through the Glue catalog. Dispatch: `streaming-data-engineer`.
- **Multi-exchange adapter framework.** Generalizing `BinanceWebSocketAdapter` so a second source plugs in without copy-paste. Dispatch: `backend-engineer`.

Until these land, the production-reference profile is **scaffolded** — a reader can `terraform apply` and `helm install` and get a running deployment, but the cloud-side advanced features (Iceberg writes, Trino queries, multi-exchange) require the application updates above.

## Operator checklist

Per [RUNBOOK.md](RUNBOOK.md), after every migration step:

- [ ] Updated the migration date in `docs/steering/RUNBOOK.md` operator notes.
- [ ] Recorded broker versions, Iceberg snapshot ids, and Glue catalog names.
- [ ] Confirmed Prometheus alerts are routed to the new cloud notification target.
- [ ] Verified backup jobs are running against RDS and S3.
- [ ] Ran the `cloud` smoke test from outside the VPC (operator workstation) to confirm ingress.
