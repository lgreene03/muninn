# 0003. Managed Kafka via Amazon MSK (production-reference)

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Project maintainer
- **Related:** [docs/steering/TECH_STACK.md](../steering/TECH_STACK.md), [docs/steering/ROADMAP.md](../steering/ROADMAP.md) §Phase 8, `local-infra/terraform/aws/modules/msk/`

## Context

[ROADMAP.md](../steering/ROADMAP.md) §Phase 8 ("Production-Reference Architecture") calls for the scaled-up topology to use managed Kafka. The local profile runs single-node Redpanda — wire-compatible with Kafka — so the application code does not change. The question is which managed offering to target in the reference Terraform.

The candidates and their tradeoffs:

| Option | Pros | Cons |
|---|---|---|
| Amazon MSK | Native AWS integration (IAM, KMS, CloudWatch, VPC peering). No new vendor relationship. Predictable price. | Slower minor-version cadence than self-managed. EBS-only storage. Brokers exposed as nodes (operator must reason about partitions). |
| MSK Serverless | No broker sizing decisions. Pay-per-throughput. | Higher unit cost at small scale. Some Kafka features unavailable. Throughput caps need monitoring. |
| Redpanda Cloud | Wire-compatible with our local Redpanda, identical mental model. Strong p99 latency claims. | New vendor relationship. Pricing tier model. Less integrated with AWS IAM. |
| Confluent Cloud | Largest Kafka ecosystem (Schema Registry, ksqlDB, Connect). | Pricing premium. Most lock-in to vendor APIs. |
| Self-managed Kafka on EKS | Total control. | Operates a broker fleet — counter to the "production-shaped, not production-heavy" principle. |

## Decision

The Phase 8 production-reference profile uses **Amazon MSK (provisioned)** as the Kafka platform. The Terraform module lives at [`local-infra/terraform/aws/modules/msk/`](../../local-infra/terraform/aws/modules/msk/).

Defaults:
- 3 broker nodes (single-AZ tolerant; cluster spans the 3 private subnets created by the VPC module).
- `kafka.t3.small` for the reference / cheap-cloud deployment; bump to `m5.large+` for sustained throughput.
- TLS-only client-broker traffic. Plaintext is rejected at the cluster level.
- AWS-managed KMS key at rest by default; pin a CMK via `kms_key_arn` for compliance.
- CloudWatch broker logs, retention 14 days.
- `PER_BROKER` enhanced monitoring (enough to drive the pipeline-overview dashboard from [OBSERVABILITY_STRATEGY.md](../steering/OBSERVABILITY_STRATEGY.md)).
- Security group ingress bounded to the VPC CIDR (and any opt-in peered CIDRs); no public exposure.

## Rationale

- **MSK keeps the AWS-native posture.** EKS, S3, Glue, and IAM are already chosen; MSK closes the loop with one IAM-aware control plane.
- **The application is broker-agnostic.** Muninn talks plain Kafka protocol; switching to Redpanda Cloud later is a `bootstrap.servers` change plus credential reshuffling, not a code change.
- **Provisioned over Serverless at MVP scale.** At the BTC-USDT volume the project is sized for, three small brokers cost less than serverless-billed throughput. Re-evaluate when measured throughput justifies it.
- **TLS is non-negotiable for any out-of-VPC client.** The previous module's `TLS_PLAINTEXT` setting was a placeholder; the production-reference profile requires TLS.

## Consequences

**Easier.** A single `terraform apply` provisions the broker cluster with sane defaults; secrets and CIDRs are surfaced as variables. The application's `bootstrap.servers` becomes the MSK endpoint; no code change.

**Harder.** Kafka version bumps are gated on AWS's supported-version list (less aggressive than Redpanda's cadence). Broker-level OS access is not available — operational debugging is via CloudWatch and the AWS console.

**Migration path.** See [PHASE8_MIGRATION.md](../steering/PHASE8_MIGRATION.md) for the Redpanda → MSK cutover steps. The short version: dual-write from the application during the cutover window, switch consumers, drain Redpanda, decommission.

## Alternatives Considered

- **Redpanda Cloud.** Rejected for the reference profile because it adds a vendor relationship without removing any cost. Local Redpanda already validates the mental model; cloud-side Redpanda becomes redundant. Retain as a documented swap-out for operators who prefer it.
- **Confluent Cloud.** Rejected on pricing and lock-in. The Schema Registry and Connect ecosystem are valuable but unused by Muninn today.
- **Self-managed Kafka on EKS.** Rejected — managing brokers is an explicit non-goal of the production-reference profile ("production-shaped, not production-heavy" in [ARCHITECTURE_PRINCIPLES.md](../steering/ARCHITECTURE_PRINCIPLES.md)).

## References

- [`local-infra/terraform/aws/modules/msk/`](../../local-infra/terraform/aws/modules/msk/) — the module.
- AWS MSK [supported Kafka versions](https://docs.aws.amazon.com/msk/latest/developerguide/supported-kafka-versions.html).
- [PHASE8_MIGRATION.md](../steering/PHASE8_MIGRATION.md) — operational migration playbook.
