# SECURITY_MODEL.md

This document describes Muninn's threat model: what trust boundaries exist, what assumptions hold, and what defenses are in place (or deliberately absent). It complements [SECURITY.md](../../SECURITY.md), which covers vulnerability reporting.

Muninn is **not** hardened for adversarial environments in the MVP. The threat model below is honest about that. Operators who deploy beyond a local development machine must add hardening (see §Hardening Notes).

## Asset Inventory

The things Muninn handles, in order of sensitivity:

1. **Event log integrity.** The append-only sequence of facts. Loss or tampering breaks every downstream property.
2. **Feature outputs.** Derived from the event log; reproducible by replay, but expensive to recompute at scale.
3. **Replay determinism.** A system property, not a stored asset, but defended in the same model: if an attacker can introduce non-determinism, audit and reproducibility break.
4. **Source credentials.** Read-only API keys to upstream exchange feeds (when authenticated APIs are used).
5. **Operational metadata.** Replay-job state, feature definitions, instrument reference data in PostgreSQL.

Muninn does **not** hold:

- Personally identifiable information.
- Payment or banking details.
- Customer data of any kind.
- Write credentials to any external system.

This narrow scope is deliberate and lowers the threat surface materially.

## Trust Boundaries

```
+----------------+    Untrusted: payloads, timestamps, sequence
| Exchange feeds | -- numbers, reconnect behavior. May be drifted
|  (external)    |    or malicious.
+----------------+

       |  Boundary 1: ingestion adapter parses + validates
       v

+----------------+    Trusted-after-validation: canonical
| Event log      |    MarketEvents that passed EventValidator.
| (Redpanda)     |    Untrusted: nothing — the log is the
+----------------+    canonical version.

       |  Boundary 2: in-process method calls
       v

+----------------+    Trusted: pure-function code,
| Feature engine |    deterministic inputs, no network IO.
+----------------+

       |  Boundary 3: outputs to broker, MinIO, Postgres
       v

+----------------+    Trusted: derived events with full provenance.
| Outputs        |
+----------------+

       |  Boundary 4: query API
       v

+----------------+    Untrusted: HTTP clients. May be local users,
| HTTP clients   |    notebooks, dashboards, or anything else.
+----------------+
```

Each boundary has a defense:

| Boundary | Defense |
|---|---|
| Source → ingestion | `EventValidator` rejects malformed payloads to dead-letter |
| Ingestion → log | Idempotent Kafka producer; `acks=all`; schema versioning |
| Log → feature engine | Deserialization with strict typing; rejected events surface as engine-side validation errors |
| Engine → outputs | Pure-function guarantees; ArchUnit forbids non-deterministic patterns |
| HTTP → query API | Input validation; read-only endpoints; no auth in MVP (operator's responsibility) |

## Threat Catalog

### T1. Malformed payloads from a compromised or drifted source

**Threat.** An exchange feed sends events with corrupted timestamps, unparseable numerics, missing required fields, or unexpected new fields.

**Mitigation.**
- `EventValidator` rejects anything that doesn't match the canonical record shape.
- Rejected events go to `events.deadletter` with a structured reason — they are not silently dropped.
- Golden-file contract tests catch schema drift in adapter parsers at PR time.

**Residual risk.** A subtly malicious source could craft payloads that pass validation but contain semantically wrong values (e.g., a trade with the correct shape but a negative price below the validator's lower bound — which the validator catches; or a value just inside the bound). This is bounded by validator rules; widening the bounds widens the risk.

### T2. Replay-time poisoning

**Threat.** An attacker with write access to the broker or to MinIO writes fabricated events into the historical log. Subsequent replays then produce attacker-controlled outputs.

**Mitigation.**
- Broker and MinIO are not exposed to the public internet by default.
- Producer credentials are not used by any code outside the ingestion service.
- Sequence numbers and event IDs (UUIDv7) make insertions detectable by gap analysis.

**Residual risk.** Significant if an attacker breaches the operator's network. Production deployments must restrict write access to the broker to the ingestion service identity only.

### T3. Non-determinism leak

**Threat.** A code change (well-intentioned or malicious) introduces non-determinism into the feature engine — a wall-clock read, an unseeded random, a HashMap iteration — and replay outputs silently diverge from live.

**Mitigation.**
- Forbidden patterns enforced by ArchUnit at build time ([CODING_STANDARDS.md](CODING_STANDARDS.md)).
- Determinism tests in `VwapDeterminismTest` and equivalents for every feature.
- Shadow-replay divergence detection emits `muninn.replay.divergence.detected` — alerted on.

**Residual risk.** A non-determinism source that escapes both ArchUnit and the tests would only surface in production via the divergence metric. The metric is monitored; the response is in [RUNBOOK.md](RUNBOOK.md) §Feature Engine.

### T4. Query API abuse

**Threat.** A client sends queries that cause excessive DuckDB scans, exhausting memory or CPU.

**Mitigation.**
- DuckDB memory limits per profile.
- Per-request timeouts.
- Pagination caps on result-set size.

**Residual risk.** Without authentication (MVP), any caller of the API can exhaust resources. Operators exposing the API beyond localhost must put authentication and rate limiting in a reverse proxy.

### T5. Configuration leakage

**Threat.** Secrets (exchange API keys, broker credentials, MinIO credentials) end up in committed code, logs, or PR descriptions.

**Mitigation.**
- All secrets via environment variables, never in `application.yml`.
- The `@Sensitive` annotation (planned) marks fields whose `toString()` returns `***`.
- Logging configuration rejects known credential-shaped patterns.
- `.gitignore` excludes `.env` and any local-only config.

**Residual risk.** Human error. Pre-commit hooks (planned) will scan for likely secrets.

### T6. Supply-chain compromise

**Threat.** A malicious version of a dependency is published and consumed.

**Mitigation.**
- Pinned versions in `pom.xml`; no version ranges.
- Dependabot enabled on the GitHub repo (operator setting).
- OWASP dependency-check planned in CI.

**Residual risk.** Zero-day in a pinned dependency between detection and patch. Triage via [SECURITY.md](../../SECURITY.md).

### T7. Local-disk exhaustion

**Threat.** Disk fills, Redpanda dies, ingestion stops, events are lost.

**Mitigation.**
- Retention policies in [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md).
- Disk-usage alerts in [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md) (planned).

**Residual risk.** Local-first means local-disk-limited. Operators must monitor.

## Out of Scope (MVP)

The following are not defended against in MVP and require operator action if relevant:

- Multi-tenant isolation.
- Network-level threats (DDoS, MITM, sniffing).
- Authentication and authorization on any API.
- Encrypted at-rest storage (depends on host volume encryption).
- Audit logging of who-did-what (the log is event-data, not action-data).
- Compliance regimes (SOC2, GDPR, MiFID II, etc.).

These are explicitly listed so that an operator knows what they own.

## Hardening Notes for Operators

If you run Muninn outside a developer laptop, do at least:

1. **Network isolation.** Redpanda, PostgreSQL, MinIO ports never publicly reachable. Bind to internal interfaces only.
2. **TLS everywhere.** Front the query API and the ingestion API with a reverse proxy that terminates TLS.
3. **Authentication on every external API.** Even read-only endpoints. A simple bearer token in the reverse proxy is enough for a private deployment.
4. **Secret management.** Use environment variables or a secret store. Never put secrets in committed config.
5. **Volume encryption.** Encrypt the host's filesystem so disk theft does not expose the event log.
6. **Backups.** Snapshot MinIO and PostgreSQL daily. Test restores quarterly.
7. **Patch cadence.** Apply Dependabot updates within the SLAs from [SECURITY.md](../../SECURITY.md).

These are operational hardening steps. They are not part of the MVP code. Their absence in code is by design — Muninn is built to be deployable in many environments, not opinionated about which.

### Additional hardening for the production-reference (AWS) profile

The Terraform modules under `local-infra/terraform/aws/` ship with these defaults; operators should not weaken them without a specific, written reason.

- **MSK security group bounded to the VPC CIDR.** No `0.0.0.0/0` ingress. Peered VPC or VPN access goes through `additional_ingress_cidrs`.
- **MSK encryption-in-transit set to `TLS`, not `TLS_PLAINTEXT`.** Plaintext client-broker traffic is refused.
- **MSK at-rest encryption uses the AWS-managed KMS key by default.** Pin a customer-managed key via `kms_key_arn` when compliance requires controlled rotation.
- **S3 warehouse bucket has server-side encryption (AES256), versioning enabled, public-access blocked, and lifecycle policies that bound storage cost.**
- **S3 `force_destroy` defaults to `false`.** Setting it to `true` is acceptable only for ephemeral dev / integration-test environments. Production deletes go through versioned object handling, not bucket teardown.
- **EKS worker nodes private.** The control-plane endpoint is the only public surface; lock it down further via `endpoint_private_access` when feasible.
- **IRSA (IAM Roles for Service Accounts)** binds the application's pod identity to S3 / Glue / CloudWatch permissions narrowly. Avoid a single broad role across all services; use one role per service.
- **CloudWatch broker logs retained.** Auth failures and broker restarts are auditable without shelling into a private subnet.

These defaults reflect [ADR-0003](../adr/0003-managed-kafka-via-msk.md) and [ADR-0005](../adr/0005-iceberg-with-glue-catalog.md). Changes that relax them require an ADR.

## Reporting

Security issues are reported per [SECURITY.md](../../SECURITY.md). The threat model in this document is the lens through which reports are triaged.
