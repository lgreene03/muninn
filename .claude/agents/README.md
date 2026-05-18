# Muninn Specialist Team

Custom Claude Code subagents that own focused slices of Muninn's work. Each agent has a sharp scope, an explicit list of non-responsibilities, and the steering docs it must read before acting.

## Roster

| Agent | Beat | When to dispatch |
|---|---|---|
| [`backend-engineer`](backend-engineer.md) | Java/Spring/Kafka/JPA/DuckDB | New REST endpoints, controllers, exchange adapters, Query API work, bean wiring |
| [`streaming-data-engineer`](streaming-data-engineer.md) | Feature engine internals, Parquet, Iceberg | Windows, watermarks, checkpoints, new feature definitions, archival path |
| [`frontend-engineer`](frontend-engineer.md) | Demo dashboard (HTML/JS) | Phase 7 dashboard, charts, replay-job UI |
| [`devops-sre`](devops-sre.md) | Docker, Compose, CI, observability stack, K8s, Terraform | Phase 6 (Prometheus/Grafana/Tempo), Phase 8 production-reference |
| [`qa-engineer`](qa-engineer.md) | Test discipline | Determinism tests, golden datasets, integration scenarios, JMH benchmarks, ArchUnit rules |
| [`security-engineer`](security-engineer.md) | Threat model, deps, validation, hardening notes | Dependency audits, secret scanning, validator rules, operator guidance |
| [`technical-writer`](technical-writer.md) | Steering docs, ADRs, runbook, changelog | Any doc work, ADR drafting, doc-drift fixes |
| [`developer-advocate`](developer-advocate.md) | Demo scripts, blog posts, talks, README polish | Public-facing artifacts — never marketing fluff |
| [`product-shepherd`](product-shepherd.md) | Roadmap pacing, scope discipline, NON_GOALS guardrail | Phase audits, triage of new ideas, PR-scope review |

## Dispatch Guide by Roadmap Phase

| Phase | Primary | Supporting |
|---|---|---|
| **5 — Query API** | `backend-engineer` | `qa-engineer`, `technical-writer`, `security-engineer` |
| **6 — Observability** | `devops-sre` | `streaming-data-engineer` (metrics emission), `qa-engineer`, `technical-writer` |
| **7 — Polish + Demo** | `developer-advocate`, `frontend-engineer` | `technical-writer`, `devops-sre` (cloud-cheap profile) |
| **8 — Production-Reference** | `devops-sre` | `backend-engineer`, `streaming-data-engineer` (Iceberg), `security-engineer`, `technical-writer` |

`product-shepherd` and `qa-engineer` are dispatched across every phase as needed.

## How to Dispatch

From the main thread, use the Agent tool with `subagent_type` set to the agent name:

```
Agent({
  subagent_type: "backend-engineer",
  description: "Implement Query API feature-timeseries endpoint",
  prompt: "<self-contained task brief — file paths, exit criteria, test layers expected>"
})
```

Each agent expects a self-contained brief that names files, references the relevant steering doc, and states the exit criterion. Agents do not see prior conversation history; the prompt is the whole context.

## Parallel vs Sequential

Agents whose work is independent can run in parallel — send multiple Agent tool calls in a single message. Examples:

- `devops-sre` (Grafana dashboards) and `backend-engineer` (Query API) in parallel during Phase 6.
- `technical-writer` (ADR drafting) and `qa-engineer` (test hardening) in parallel almost any time.

Agents whose work depends on another must run sequentially:

- `frontend-engineer` waits for `backend-engineer` to land the API endpoint.
- `developer-advocate` waits for `technical-writer` to settle the facts.

## Shared Discipline

Every agent reads from the same source of truth:

- [AGENTS.md](../../AGENTS.md)
- [docs/steering/ARCHITECTURE_PRINCIPLES.md](../../docs/steering/ARCHITECTURE_PRINCIPLES.md)
- [docs/steering/NON_GOALS.md](../../docs/steering/NON_GOALS.md)
- [docs/steering/AI_AGENT_WORKFLOW.md](../../docs/steering/AI_AGENT_WORKFLOW.md)

Every agent follows the same loop: **READ → PLAN → TEST → CODE → DOC → SUMMARIZE.**

## Adding a New Specialist

If a real need arises that none of the existing agents covers:

1. Confirm the work is recurring, not one-off. One-off tasks go to `claude` or `general-purpose`.
2. Confirm an existing agent can't absorb the scope. Specialization has a cost.
3. Draft the agent file using one of the existing files as a template. Required sections:
   - YAML frontmatter (`name`, `description`, `tools`, `model`).
   - "Before Editing Anything" — what to read first.
   - "In Scope" / "Out of Scope".
   - "Non-Negotiables".
   - "When Done" — what the summary should contain.
4. Add an entry to this README.
5. Have `product-shepherd` validate the scope doesn't overlap with existing roles.
