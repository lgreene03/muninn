# Muninn Specialist Team

Nine custom Claude Code subagents that own focused slices of Muninn's work.

This file is the team manual: roster, dispatch guide, handoff matrix, and the brief template every dispatch should follow.

## Design Principles

The structure of each agent file is informed by Anthropic's own engineering writeups on multi-agent systems:

- **[How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system)** — subagents need an objective, output format, tool guidance, and clear task boundaries; vague instructions cause duplicate work; effort is allocated proportionally to task complexity.
- **[Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)** — minimal viable tool sets; heuristics over rigid rules; organized sections; honest scope.

Each agent file therefore contains:

| Section | Purpose |
|---|---|
| **Objective** | One sentence stating purpose |
| **When to Dispatch** | Concrete triggers; prevents vague delegation |
| **Required Reading** | Steering docs in priority order — shared context |
| **Scope** (In / Out + handoff routing) | Context isolation; explicit hand-off targets |
| **Heuristics** | Strategies, not rules. The interesting judgment |
| **Non-Negotiables** | The small set of hard rules |
| **Common Failure Modes** | Role-specific mistakes to avoid |
| **Effort Budgets** | Proportional allocation by task shape |
| **Output Format** | Structured summary the orchestrator can act on |

Tool sets are curated, not maximal. `technical-writer` and `developer-advocate` don't have `Bash` — they don't build anything. `security-engineer` and `developer-advocate` get `WebSearch` / `WebFetch` because their work is research-heavy.

## Roster

| Agent | Beat | Tools | When to dispatch |
|---|---|---|---|
| [`backend-engineer`](backend-engineer.md) | Java/Spring/Kafka/JPA/DuckDB | Bash, Read, Edit, Write, Glob, Grep | REST endpoints, controllers, exchange adapters, Query API |
| [`streaming-data-engineer`](streaming-data-engineer.md) | Feature engine, watermarks, checkpoints, Parquet, Iceberg | Bash, Read, Edit, Write, Glob, Grep | Windowing, new features, archival, Iceberg migration |
| [`frontend-engineer`](frontend-engineer.md) | Demo dashboard (HTML/JS) | Bash, Read, Edit, Write, Glob, Grep | Phase 7 dashboard, charts, replay-job UI |
| [`devops-sre`](devops-sre.md) | Compose, CI, observability stack, K8s, Terraform | Bash, Read, Edit, Write, Glob, Grep, WebFetch | Phase 6 observability, Phase 8 production-reference |
| [`qa-engineer`](qa-engineer.md) | Test discipline across seven layers | Bash, Read, Edit, Write, Glob, Grep | Determinism tests, golden datasets, integration scenarios, ArchUnit rules |
| [`security-engineer`](security-engineer.md) | Threat model, deps, validation, hardening notes | Bash, Read, Edit, Write, Glob, Grep, WebFetch, WebSearch | Dependency audits, validator rules, operator hardening |
| [`technical-writer`](technical-writer.md) | Steering docs, ADRs, runbook, changelog | Read, Edit, Write, Glob, Grep | Any doc work, ADR drafting, doc-drift fixes |
| [`developer-advocate`](developer-advocate.md) | Demo scripts, blog posts, talks, README polish | Read, Edit, Write, Glob, Grep, WebFetch, WebSearch | Public-facing artifacts |
| [`product-shepherd`](product-shepherd.md) | Roadmap pacing, scope discipline, NON_GOALS guardrail | Bash, Read, Edit, Write, Glob, Grep | Phase audits, triage, PR-scope review |

## Dispatch Guide by Roadmap Phase

| Phase | Primary | Supporting | Wraps with |
|---|---|---|---|
| **5 — Query API** | `backend-engineer` | `qa-engineer`, `security-engineer` | `technical-writer` (OpenAPI prose + ADRs) |
| **6 — Observability** | `devops-sre` | `streaming-data-engineer` (metric emission) | `qa-engineer` (dashboard smoke + alert tests), `technical-writer` (RUNBOOK additions) |
| **7 — Polish + Demo** | `frontend-engineer`, `developer-advocate` | `devops-sre` (cloud-cheap profile), `technical-writer` | `product-shepherd` (Phase 7 exit audit) |
| **8 — Production-Reference** | `devops-sre` | `backend-engineer`, `streaming-data-engineer` (Iceberg), `security-engineer` | `technical-writer` (multiple ADRs), `product-shepherd` (phase audit) |

`product-shepherd` and `qa-engineer` are dispatched across every phase as needed.

## Handoff Matrix

When an agent hits something outside its scope, route the work as follows. Pulled from each agent's own "Out of scope" table — repeated here for orchestrator visibility.

| Need | Route to |
|---|---|
| Java/Spring/Kafka/JPA code | `backend-engineer` |
| Feature engine internals, determinism property | `streaming-data-engineer` |
| Dashboard, HTML, JS | `frontend-engineer` |
| Compose, CI, K8s, Terraform, observability infra | `devops-sre` |
| Tests of any layer | `qa-engineer` |
| Threat model, validator rules, dependency audit | `security-engineer` |
| Steering docs, ADRs, runbook, package-info | `technical-writer` |
| Demo, README polish, blog post, social copy | `developer-advocate` |
| Scope question, "should we do X?", roadmap update | `product-shepherd` |
| New dependency (validation) | `product-shepherd` first, then the relevant engineer |

## Dispatch Brief Template

Anthropic's research finding: vague briefs cause subagents to misinterpret tasks or duplicate work. Every dispatch should follow this shape:

```
Goal: <single sentence>

Context:
  Phase: <5/6/7/8 or "ongoing">
  Related docs: <paths>
  Related code: <paths if known>
  Why now: <link to phase exit criterion, ADR, issue>

Inputs the agent has:
  - <file or doc>
  - <interface contract>

Exit criterion:
  <how the orchestrator will know it's done — observable, not "complete">

Effort budget: <small | medium | phase-deliverable — see the agent's file>

Out-of-scope:
  - <anything explicitly off-limits>

Hand-off expectations:
  - On finish, return the agent's Output Format.
  - Queue: <any downstream agent that should pick up after>
```

If you can't fill all of these for a task, the task isn't ready — it's `product-shepherd`'s.

## Parallel vs Sequential

Anthropic's research: parallel subagents cut research time by up to 90% on complex queries. Apply the same principle here.

Run in **parallel** (single message, multiple Agent calls) when work is independent:

- `devops-sre` building Grafana dashboards + `backend-engineer` implementing Query API during Phase 6.
- `technical-writer` drafting an ADR + `qa-engineer` adding a determinism test for the same feature.
- `security-engineer` auditing dependencies + `streaming-data-engineer` adding a new feature.

Run **sequentially** when there's a real dependency:

- `frontend-engineer` waits for `backend-engineer` to land the API endpoint (and the OpenAPI spec).
- `developer-advocate` waits for `technical-writer` to settle the facts.
- `qa-engineer` writes the integration test after the implementation lands (or in parallel with a stub if the contract is fixed).

## Shared Discipline

Every agent reads from the same source of truth:

- [AGENTS.md](../../AGENTS.md)
- [docs/steering/ARCHITECTURE_PRINCIPLES.md](../../docs/steering/ARCHITECTURE_PRINCIPLES.md)
- [docs/steering/NON_GOALS.md](../../docs/steering/NON_GOALS.md)
- [docs/steering/AI_AGENT_WORKFLOW.md](../../docs/steering/AI_AGENT_WORKFLOW.md)

Every agent follows the same loop: **READ → PLAN → TEST → CODE → DOC → SUMMARIZE.**

## Adding a New Specialist

Specialization has a cost. Before adding an agent:

1. Confirm the work is recurring, not one-off. One-off tasks go to `claude` or `general-purpose`.
2. Confirm an existing agent can't absorb the scope.
3. Confirm the role has a beat that doesn't overlap any current agent's "In scope" table.
4. Draft the agent file using one of the existing files as a template. All nine sections required.
5. Curate the tool set. Default to fewer tools than you think.
6. Pick a model (sonnet is the default; bump to opus only when a role's correctness is high-stakes and call frequency is low).
7. Add to this README's roster, dispatch guide, and handoff matrix.
8. Have `product-shepherd` validate the scope doesn't overlap with existing roles.

## Operating the Team

For the orchestrator (the main thread):

- **Don't over-decompose.** A typo fix doesn't need three agents.
- **Don't under-decompose.** A phase deliverable that fits one agent's scope cleanly should go to that one agent — but a phase that spans several should be sequenced explicitly.
- **Believe the agent's summary, but verify.** Each agent's Output Format names what it tested. Confirm those files exist and CI is green before declaring done.
- **Bounce scope questions to `product-shepherd`** before assuming. Five minutes of triage saves an hour of rework.

## Sources Informing This Design

- Anthropic — [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system).
- Anthropic — [Effective context engineering for AI agents](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents).
- Anthropic — [Claude Code best practices](https://www.anthropic.com/engineering/claude-code-best-practices).
