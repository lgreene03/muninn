# 0006. Production Deployment Topology

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Antigravity (AI Orchestrator), lgreene03 (Operator)
- **Related:** [docs/steering/ROADMAP.md](../steering/ROADMAP.md), [docs/steering/LOCAL_FIRST_CONSTRAINTS.md](../steering/LOCAL_FIRST_CONSTRAINTS.md)

## Context

Muninn began life as a local-first, single-JVM monorepository to enforce low latency, zero infrastructure costs, and simple debugging. However, as the research platform scales to consume more exchange feeds, concurrently compute complex rolling analytics, and support multiple research notebooks querying the Parquet warehouse, the single-node local deployment profile becomes inadequate for production workloads.

To transition to production without rewriting our core event-native processing pipelines or breaking the local developer workflow, we require a scalable, automated, and declarative deployment blueprint for AWS.

## Decision

We accept EKS (Elastic Kubernetes Service) as the target compute platform, MSK (Managed Streaming for Apache Kafka) as the high-throughput, append-only event ledger, Helm as our application packaging manager, and Terraform as our Infrastructure-as-Code (IaC) orchestrator.

The application services (`ingestion-service`, `feature-engine`, `replay-engine`, `query-api`) are packaged in a parameterized, unified Helm chart and deployed as distinct, horizontally-scaled pods inside EKS namespaces, connecting to shared MSK brokers and S3 buckets.

## Consequences

*   **What becomes easier:**
    *   **Independent Scaling:** Read-heavy analytics (`query-api`) can scale out using Horizontal Pod Autoscaling (HPA) without affecting event ingestion.
    *   **High Availability:** MSK replicates events across multiple Availability Zones automatically, eliminating single-node database corruption risks.
    *   **Infrastructure Reproducibility:** Spin up identical staging and production environments using Terraform with zero manual configuration.
*   **What becomes harder:**
    *   **Operational Overhead:** Managing EKS node groups, broker IAM authorization, and VPC private NAT gateways increases monitoring and operational needs.
    *   **Cloud Costs:** Staging and production AWS topologies incur substantial monthly bills compared to our zero-cost local footprint.
*   **Consequences & Follow-up:**
    *   Continuous integration must execute Terraform validation and Helm chart lints on every commit to prevent deployment drift.
    *   `cloud-cheap` remains as our low-cost VPS proxy profile to buffer cost margins.

## Alternatives Considered

*   **Managed ECS (Elastic Container Service).** Rejected due to less vibrant community ecosystem support for complex analytical tools (like Trino operators) compared to EKS.
*   **Self-Managed Kafka in EC2.** Rejected due to extensive administration, patching, and partition replication management overhead.
*   **Status quo (Docker Compose only).** Rejected because it offers no dynamic high availability, horizontal scaling, or isolated multi-service networking at production scale.

## References

- [EKS Best Practices](https://aws.github.io/aws-eks-best-practices/)
- [Amazon MSK Developer Guide](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html)
- [Helm Packaging Standards](https://helm.sh/docs/chart_best_practices/)
