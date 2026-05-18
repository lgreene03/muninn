# 0004. EKS (managed node groups) over Fargate-only

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Project maintainer
- **Related:** [docs/steering/ROADMAP.md](../steering/ROADMAP.md) §Phase 8, [docs/steering/PERFORMANCE_BUDGETS.md](../steering/PERFORMANCE_BUDGETS.md), `local-infra/terraform/aws/modules/eks/`, `deploy/helm/muninn/`

## Context

Phase 8 deploys Muninn's four services (ingestion, feature engine, replay engine, query API) on Kubernetes. AWS offers three viable execution models:

| Model | Pros | Cons |
|---|---|---|
| EKS with managed node groups (EC2) | Predictable per-node cost; supports JVM-tuning via `requests`/`limits`; full DaemonSet support for logging/metrics agents; easy pod-to-pod local scheduling for stateful workloads | Operator owns node patching cadence (mitigated by managed groups) |
| EKS with Fargate only | No node management; per-pod billing | DaemonSets and some CSI drivers unsupported; pod startup latency higher; cost-per-pod higher at always-on workloads |
| ECS Fargate | Simpler than EKS for small fleets | Diverges from the upstream Kubernetes ecosystem and our Helm chart investment |

The Helm chart at [`deploy/helm/muninn/`](../../deploy/helm/muninn/) already targets Kubernetes-native primitives (Deployments, Services, HPA, Ingress). Splitting to ECS would mean rewriting the deployment story.

The four Muninn services are all **always-on**: ingestion is a continuous WebSocket consumer; the feature engine consumes Kafka in a tight loop; the replay engine and query API serve requests. None of them have the spiky, short-lived workload pattern Fargate is optimized for.

## Decision

The production-reference profile uses **Amazon EKS with managed node groups**. The Terraform module lives at [`local-infra/terraform/aws/modules/eks/`](../../local-infra/terraform/aws/modules/eks/) and the application is deployed via [`deploy/helm/muninn/`](../../deploy/helm/muninn/).

Defaults:
- `m5.large` instance type (2 vCPU, 8 GB RAM) — enough headroom for two Muninn services per node plus DaemonSets.
- Desired capacity 3, spread across three private subnets.
- Worker nodes private; only the control plane endpoint is public (and may be locked down via `endpoint_private_access` for production).

Fargate is documented as an option for the `cloud-cheap` profile (single-instance VPS or single-tenant Fargate pod) but not the primary path.

## Rationale

- **Always-on workloads make EC2 cheaper.** Fargate's per-pod premium does not pay off when pods run continuously.
- **DaemonSet support matters.** Prometheus node-exporter, FluentBit (or AWS Distro for OpenTelemetry), CSI drivers — Phase 6 observability assumes these.
- **JVM resource tuning is finer-grained on EC2.** `MaxRAMPercentage` and G1 heuristics work better when the pod has a predictable absolute memory cap and a homogeneous host.
- **The Helm chart already exists for Kubernetes.** Switching execution model would not save investment elsewhere.

## Consequences

**Easier.** The Helm chart deploys as-is. HPA, Ingress, ConfigMaps, Secrets — all standard. CloudWatch Container Insights, ALB Controller, and EBS CSI install via the usual add-ons.

**Harder.** Node patching is the operator's responsibility (managed node groups automate the rolling-update mechanic; the *cadence* is operator choice). Cluster autoscaler must be configured separately if dynamic node scaling is needed.

**Cost.** Three `m5.large` nodes at on-demand pricing is ~$200/month; spot or savings-plan reductions are an operator concern. The `cloud-cheap` profile remains the answer for budgets below that.

## Alternatives Considered

- **EKS Fargate-only.** Rejected for the production-reference profile because of the always-on workload pattern and DaemonSet limitations. Kept as an option for the `cloud-cheap` profile where simplicity outweighs cost.
- **ECS Fargate.** Rejected because the Helm-chart investment would be discarded and the K8s ecosystem (Prometheus, OpenTelemetry, ArgoCD) is the broader infrastructure target.
- **Self-managed Kubernetes (kops, kubeadm).** Rejected — counter to "production-shaped, not production-heavy".
- **GKE / AKS.** Documented as a future migration target. The Helm chart is portable; only the Terraform module changes shape.

## References

- [`local-infra/terraform/aws/modules/eks/`](../../local-infra/terraform/aws/modules/eks/)
- [`deploy/helm/muninn/`](../../deploy/helm/muninn/)
- [PERFORMANCE_BUDGETS.md §Memory Budgets](../steering/PERFORMANCE_BUDGETS.md#memory-budgets).
