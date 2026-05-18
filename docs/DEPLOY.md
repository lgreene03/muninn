# DEPLOY.md — Production-Reference Deployment

This document walks through deploying Muninn to AWS via Terraform + Helm. It targets the **`production-reference`** profile defined in [LOCAL_FIRST_CONSTRAINTS.md](steering/LOCAL_FIRST_CONSTRAINTS.md).

For the architectural rationale behind these choices, see [ADR-0003 (MSK)](adr/0003-managed-kafka-via-msk.md), [ADR-0004 (EKS)](adr/0004-eks-over-fargate-only.md), and [ADR-0005 (Iceberg + Glue)](adr/0005-iceberg-with-glue-catalog.md).

For migrating an existing local deployment, follow [PHASE8_MIGRATION.md](steering/PHASE8_MIGRATION.md) instead. This document is the *fresh deploy* path.

## What gets deployed

```
                      VPC (10.0.0.0/16)
                      │
                      ├── 3 × public subnet  (ALB)
                      │
                      ├── 3 × private subnet (workloads)
                      │       │
                      │       ├── EKS managed node group  ── Helm release: muninn
                      │       │     ├── ingestion       (Deployment)
                      │       │     ├── feature-engine  (Deployment)
                      │       │     ├── replay-engine   (Deployment)
                      │       │     └── query-api       (Deployment + HPA + Ingress)
                      │       │
                      │       └── MSK cluster (3 brokers, TLS-only)
                      │
                      └── Endpoints
                            ├── S3 (warehouse bucket)
                            ├── Glue Data Catalog (Iceberg metadata)
                            ├── RDS PostgreSQL (metadata)
                            └── ECR (private images)
```

## Prerequisites

| Tool | Minimum | Notes |
|---|---|---|
| Terraform | 1.5+ | Uses the AWS provider 5.x family |
| AWS CLI | 2.x | Authenticated to the target account with admin-equivalent for the bootstrap |
| `kubectl` | 1.28+ | Or whatever matches the EKS version |
| `helm` | 3.13+ | |
| `jq` | latest | Used by the bundled scripts |
| `docker` | 24+ | For building images locally if not using a CI pipeline |

Assumed: an AWS account with capacity for an EKS cluster, an MSK cluster, an S3 bucket, an RDS instance, and an ECR registry. The Terraform plan stages costs; the reference defaults run at roughly the cost of a small EC2 + MSK cluster (single-AZ tolerant; not multi-AZ HA).

## Step 1 — Provision infrastructure

```bash
cd local-infra/terraform/aws

terraform init

terraform plan \
    -var aws_region=us-east-1 \
    -var environment=staging \
    -var project=muninn

terraform apply \
    -var aws_region=us-east-1 \
    -var environment=staging \
    -var project=muninn
```

The default variables are sized for the reference workload. Override `eks_node_instance_types`, `msk_instance_type`, and `msk_broker_nodes` for larger scale. See `local-infra/terraform/aws/variables.tf`.

The apply takes roughly 25 minutes — EKS and MSK both have long provisioning times. The outputs include the MSK bootstrap, S3 bucket name, RDS endpoint, EKS cluster name, and Glue database name.

```bash
terraform output -json > ../../../target/aws-outputs.json
```

## Step 2 — Build and push images

```bash
# Authenticate Docker to ECR (script assumes the registry created by Terraform).
aws ecr get-login-password --region us-east-1 \
    | docker login --username AWS --password-stdin "$ECR_REGISTRY"

# Build the application image.
mvn -B -DskipTests package
docker build -t muninn:latest .

# Tag and push for each service.
for svc in ingestion feature replay query; do
    docker tag muninn:latest "$ECR_REGISTRY/muninn-$svc:$VERSION"
    docker push "$ECR_REGISTRY/muninn-$svc:$VERSION"
done
```

The four service images are produced from the same JAR with different Spring profiles activated at startup. The Helm chart's per-service `image.repository` values point at these tags.

## Step 3 — Configure kubectl

```bash
aws eks update-kubeconfig \
    --region us-east-1 \
    --name "$(jq -r '.eks_cluster_name.value' target/aws-outputs.json)"

kubectl get nodes
```

## Step 4 — Bootstrap cluster-level resources

```bash
# Create the namespace.
kubectl create namespace muninn

# Stash secrets the Helm chart expects.
kubectl create secret generic muninn-postgres-credentials \
    -n muninn \
    --from-literal=password="$RDS_PASSWORD"

kubectl create secret generic muninn-msk-tls \
    -n muninn \
    --from-file=ca.crt=./local-infra/k8s/secrets/msk-ca.crt

# IRSA — the service account that lets pods read the S3 warehouse + write CloudWatch logs.
# This is created by the eks module's outputs; bind it before installing the chart.
kubectl annotate serviceaccount default \
    -n muninn \
    eks.amazonaws.com/role-arn="$(jq -r '.muninn_pod_role_arn.value' target/aws-outputs.json)"
```

## Step 5 — Install the Helm chart

```bash
cd deploy/helm

# Render the values with Terraform outputs.
helm install muninn ./muninn \
    --namespace muninn \
    --set "global.environment=production-reference" \
    --set "global.redpanda.bootstrapServers=$(jq -r '.msk_bootstrap_brokers_tls.value' ../../target/aws-outputs.json)" \
    --set "global.minio.endpoint=https://s3.us-east-1.amazonaws.com" \
    --set "global.minio.bucketName=$(jq -r '.warehouse_bucket.value' ../../target/aws-outputs.json)" \
    --set "global.postgres.host=$(jq -r '.rds_endpoint.value' ../../target/aws-outputs.json)" \
    --set "ingestion.image.repository=$ECR_REGISTRY/muninn-ingestion" \
    --set "feature.image.repository=$ECR_REGISTRY/muninn-feature" \
    --set "replay.image.repository=$ECR_REGISTRY/muninn-replay" \
    --set "query.image.repository=$ECR_REGISTRY/muninn-query" \
    --set "ingress.enabled=true" \
    --set "ingress.hosts[0].host=muninn.${YOUR_DOMAIN}"

kubectl get pods -n muninn -w
```

For repeated installs, capture this as `values-staging.yaml` and use `-f` rather than long `--set` lists.

### Switching the archival writer to Iceberg

The Helm chart defaults `feature.archivalSink=parquet` so the feature engine continues writing Hive-partitioned Parquet to S3 on first install. To engage the Iceberg + Glue write path (the production-reference choice per [ADR-0007](adr/0007-iceberg-feature-sink.md)):

```bash
helm upgrade muninn ./muninn \
    --namespace muninn \
    --reuse-values \
    --set "feature.archivalSink=iceberg" \
    --set "feature.iceberg.catalogType=glue" \
    --set "feature.iceberg.warehouse=s3://$(jq -r '.warehouse_bucket.value' ../../target/aws-outputs.json)" \
    --set "feature.iceberg.glueDatabase=$(jq -r '.glue_database.value' ../../target/aws-outputs.json)" \
    --set "feature.iceberg.awsRegion=us-east-1" \
    --set "feature.iceberg.schema=muninn"
```

The feature engine restarts with the Iceberg-backed sink. Tables are created on first write per `(featureName, featureVersion)` using the naming convention from [ADR-0006 §Naming](adr/0006-trino-query-backend.md). Recommended sequence:

1. Run `scripts/migrate-parquet-to-iceberg.sh` to backfill existing Parquet partitions into the new Iceberg tables.
2. Flip `feature.archivalSink=iceberg` (above).
3. Once the Trino backend reports identical results for a reference query window, flip `query.backend=trino` (below).

### Switching the Query API to Trino

The Helm chart defaults `query.backend=duckdb` so an initial install works the moment Parquet starts landing in S3. To engage the Iceberg + Trino query path (the production-reference choice per [ADR-0006](adr/0006-trino-query-backend.md)):

```bash
helm upgrade muninn ./muninn \
    --namespace muninn \
    --reuse-values \
    --set "query.backend=trino" \
    --set "query.trino.host=$(jq -r '.trino_coordinator_host.value' ../../target/aws-outputs.json)" \
    --set "query.trino.port=8080" \
    --set "query.trino.user=muninn" \
    --set "query.trino.catalog=iceberg" \
    --set "query.trino.schema=muninn" \
    --set "query.trino.ssl=true"
```

The query API restarts with the Trino-backed wiring. The `muninn.query.requests` and `muninn.query.latency` metrics now carry `backend="trino"` as a tag, so the dashboards can distinguish backends. Migrating in stages — leaving DuckDB live while Trino warms up — is supported because the two paths are wired by separate `@ConditionalOnProperty` beans.

## Step 6 — Verify

```bash
# Port-forward the query API to your workstation for the smoke check.
kubectl port-forward -n muninn svc/muninn-query 8080:8080 &

# Run the cloud-aware smoke test.
APP_URL=http://localhost:8080 ./scripts/smoke.sh --target cloud

# Check observability.
kubectl port-forward -n muninn-observability svc/grafana 3000:3000
open http://localhost:3000
```

The "pipeline overview" dashboard from [OBSERVABILITY_STRATEGY.md](steering/OBSERVABILITY_STRATEGY.md) should show ingest rate, broker lag, and feature emission rate within minutes of the first event.

## Cost expectations (reference defaults)

| Resource | Monthly cost (us-east-1, on-demand) |
|---|---|
| EKS control plane | $73 |
| 3 × `m5.large` nodes | ~$210 |
| MSK 3 × `kafka.t3.small` | ~$130 |
| RDS `db.t3.small` PostgreSQL | ~$30 |
| S3 + Glue + CloudWatch | ~$10–30 depending on retention |
| **Approximate total** | **~$450–500/month** |

The `cloud-cheap` profile (single VPS, no EKS, Redpanda Cloud free tier) targets <$30/month for the same logical workload and is documented separately when it lands.

## Tear-down

```bash
# In order — Helm release first, then Terraform.
helm uninstall muninn -n muninn
kubectl delete namespace muninn

cd local-infra/terraform/aws
terraform destroy -var environment=staging
```

The S3 bucket's `allow_destroy` variable defaults to **false** in production. Override per the [s3_iceberg module README](../local-infra/terraform/aws/modules/s3_iceberg/) when you genuinely intend to delete data.

## See also

- [PHASE8_MIGRATION.md](steering/PHASE8_MIGRATION.md) — migrating a running local deployment to cloud (rather than deploying fresh).
- [RUNBOOK.md](steering/RUNBOOK.md) — operating the deployed system.
- [SECURITY_MODEL.md](steering/SECURITY_MODEL.md) — operator hardening notes.
