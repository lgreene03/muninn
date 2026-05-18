# 0003. Local-First Telemetry Stack (Prometheus + Grafana + Tempo)

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** DevOps / SRE Specialist, Architect
- **Related:** [LOCAL_FIRST_CONSTRAINTS.md](../steering/LOCAL_FIRST_CONSTRAINTS.md), [OBSERVABILITY_STRATEGY.md](../steering/OBSERVABILITY_STRATEGY.md), [RUNBOOK.md](../steering/RUNBOOK.md)

## Context

Muninn's observability strategy requires robust, E2E distributed tracing, metric aggregation, and system resource visualization from day one. However, the system's strict [LOCAL_FIRST_CONSTRAINTS.md](../steering/LOCAL_FIRST_CONSTRAINTS.md) require it to run flawlessly on a Mac mini M4 with 24 GB of RAM, allocating a total memory budget of no more than **1 GB** for the entire observability subsystem.

Standard enterprise architectures typically use heavyweight on-premise stacks (such as Elasticsearch-Logstash-Kibana) or external cloud SaaS providers (like Datadog, Dynatrace, or New Relic).
1.  **Heavyweight Stacks (ELK/Splunk):** Elasticsearch and Logstash are highly JVM-intensive and easily exceed 4–8 GB of RAM to run stably, which violates the 1 GB local limit.
2.  **SaaS Solutions (Datadog/Sentry):** External cloud providers require persistent outbound internet connections, account setup, API token management, and introduce external dependencies, which violates the local-first, offline-capable invariant of Muninn.

We require a lightweight, modern, open-source stack that runs entirely within Docker Compose, supports standardized protocols (OpenTelemetry / W3C Trace Context / Micrometer), and consumes minimal resources.

## Decision

We adopt the **Prometheus + Grafana + Tempo** observability stack running locally under Docker Compose, utilizing tight resource constraints:

*   **Prometheus (`v2.51.1`)** for metric ingestion and alerting rules.
*   **Grafana Tempo (`v2.4.1`)** for high-efficiency distributed tracing, accepting standard OpenTelemetry and Zipkin trace protocols.
*   **Grafana (`v10.4.2`)** as the unified visualization and dashboard layer.

Each service is strictly capped in [docker-compose.observability.yml](../../docker-compose.observability.yml):
*   `prometheus`: 384 MB limit.
*   `grafana`: 256 MB limit.
*   `tempo`: 384 MB limit.

Scrape intervals are tuned to **5 seconds** for responsive local visualization, and Spring Boot Actuator exposes these endpoints natively without proprietary agents.

## Consequences

**Easier:**
*   **Zero-Dependency Local Setup:** Operators run the entire stack with a single `docker compose` command without setting up SaaS accounts or paying licensing fees.
*   **Standardized API Contracts:** Application code relies entirely on open standards (Micrometer Tracing, OpenTelemetry, W3C headers). Upgrading or replacing collectors downstream does not require modifying Spring Boot source code.
*   **Distributed Correlation:** Tracing context flows out-of-the-box from the HTTP Query/Replay endpoints, down through Kafka streams, and into the Feature Engine via OTel Zipkin collectors.

**Harder:**
*   **Resource Caps:** If the volume of events grows, memory-capped local collectors could throttle or crash. Shorter metric and trace retention periods must be strictly enforced.
*   **Port Constraints:** To prevent conflicts on standard ports like `3000` (Grafana) and `9090` (Prometheus) with existing local development daemons, we map Grafana to `3001` and Prometheus to `9091` on the host machine.

## Alternatives Considered

*   **ELK Stack (Elasticsearch, Logstash, Kibana):** Rejected. Elasticsearch is too memory-intensive and would quickly consume the laptop's RAM, starving the core streaming pipelines.
*   **Jaeger:** Considered for distributed tracing. While extremely light and highly functional, choosing Grafana Tempo allows a unified visual exploration experience (metrics-to-traces navigation) directly within the Grafana console without spinning up a separate Jaeger UI container.
*   **SaaS Telemetry Exporters:** Rejected. Fails the offline-first criteria and requires the management of sensitive API keys and external internet connections.

## References

*   [docker-compose.observability.yml](../../docker-compose.observability.yml) — Service and resource cap specifications.
*   [OBSERVABILITY_STRATEGY.md](../steering/OBSERVABILITY_STRATEGY.md) — Base blueprints and target indicators.
*   [LOCAL_FIRST_CONSTRAINTS.md](../steering/LOCAL_FIRST_CONSTRAINTS.md) — Unified hardware resource allocations.
