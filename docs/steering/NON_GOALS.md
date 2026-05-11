# NON_GOALS.md

What Muninn is **not**. These are not "maybe later"; they are deliberate exclusions.

When a contributor or AI agent proposes work that crosses one of these lines, the proposal is rejected on the basis of this document, regardless of how technically interesting it is.

---

## Muninn Is Not a Trading Bot

Muninn does not place orders, route to exchanges, manage positions, or execute strategies. It does not have a "live trading" mode. It does not have a paper-trading mode that simulates execution.

The system **observes** market events and **computes** derived features. Period.

If you want to build a trading system on top of Muninn's outputs, that is your project, in your repository.

---

## Muninn Does Not Provide Financial Advice

Nothing in this repository — code, documentation, dashboard, demo — constitutes financial advice. The system does not produce buy/sell signals as a product feature. Computed features are research artifacts, not recommendations.

This is stated in the README and reinforced anywhere the system displays computed values.

---

## Muninn Is Not a Crypto Project

Crypto-exchange APIs are used as the **initial free data source** because they are public, well-documented, available without a contract, and produce real-time event streams suitable for stress-testing event infrastructure.

Muninn is not affiliated with, endorsing, or speculating on any cryptocurrency, token, or blockchain project. The architecture serves any event-native domain: IoT telemetry, server metrics, sensor networks, exchange feeds. Crypto is the test data, not the mission.

Pull requests adding "support for token X" or "integration with chain Y" will be closed unless they fit the broader exchange-adapter framework.

---

## Muninn Is Not an HFT Engine

Muninn optimizes for **correctness, reproducibility, and observability**, not microsecond latency. Feature computation runs on the JVM with garbage collection, structured logging, and metric emission. End-to-end latency is on the order of tens of milliseconds to seconds, not microseconds.

If you need HFT, you need C++, kernel bypass, FPGA, and a colo. None of that is in scope here.

---

## Muninn Is Not a Production Trading System

The MVP is **production-shaped** (boundaries, contracts, observability, schema discipline) but not **production-heavy**. It runs on a single Mac mini M4. It has not been audited, hardened, or compliance-reviewed for any regulatory regime.

Anyone deploying Muninn for real economic decisions is doing so at their own risk, against this explicit warning.

The `production-reference` profile (Phase 8) is an architectural sketch of what scaling up looks like — not a turnkey deployment.

---

## Muninn Is Not an Autonomous Execution System

There is no agent that decides anything on the user's behalf based on Muninn's outputs. The system computes features. A human consumes them. A separate system, if it exists, acts on them. Muninn ends at the query API.

---

## Muninn Is Not Initially Multi-Exchange

The MVP supports **one** exchange adapter at a time. Multi-exchange support is a Phase 8+ concern, requiring deliberate work on:

- Exchange-specific normalization differences.
- Cross-exchange clock synchronization.
- Composite instrument modeling (the "same" instrument on different exchanges).
- Multi-source watermark logic.

None of that is built in MVP. PRs adding a second adapter before Phase 8 are out of scope.

---

## Muninn Is Not Initially Kubernetes-Based

The MVP runs on Docker Compose. Kubernetes shows up in Phase 8 as part of the `production-reference` profile, and only because it is the most common scaling target. The system is not designed *around* Kubernetes; it deploys *to* Kubernetes if and when needed.

Helm charts, operators, and CRDs are not in scope until Phase 8.

---

## Muninn Is Not a Streaming Platform Product

We do not aim to compete with Kafka, Confluent, Materialize, Tinybird, Estuary, Decodable, or any commercial streaming product. Muninn uses streaming infrastructure (Redpanda); it is not a streaming infrastructure product.

If your use case needs a managed streaming platform, use one of the above. Muninn is the **application** built on top of one.

---

## Muninn Is Not Multi-Tenant SaaS

Single deployment, single user, single tenant. The data model has no `tenant_id`. Authentication is out of scope. Billing is out of scope. The MVP is for one researcher (or one team sharing a deployment) at a time.

A multi-tenant version is a different project.

---

## Muninn Is Not a Commercial Product

There is no business model. There is no roadmap to monetization. The project is permissively licensed for the public's use and for the maintainer's portfolio. Any commercial derivative is the user's responsibility.

---

## Muninn Is Not a Real-Time Notification System

Muninn computes features. It does not push them to phones, emails, or webhooks as a primary capability. Downstream consumers can subscribe to its output topics and build notification systems if they wish. That is not part of Muninn.

---

## Why These Non-Goals Exist

Every system that ships eventually drifts toward feature accretion. The list above is the **anti-feature backlog**: things we have decided, deliberately and in advance, that we will not build, no matter how reasonable they seem in the moment.

Saying no early is how the system stays small enough to be correct.
