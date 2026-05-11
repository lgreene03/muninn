# READING_GUIDE.md

The steering documents are organized by **concern** (testing, observability, storage, etc.), not by role. This guide maps **roles** to the docs that matter most for each. Read top-to-bottom in the order shown.

## If you are a researcher or quantitative analyst

You want to define features, query their values, and understand why outputs are reproducible.

1. [README.md](../../README.md) — what Muninn is.
2. [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) — why the design exists.
3. [DOMAIN_MODEL.md](DOMAIN_MODEL.md) — the vocabulary.
4. [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md) — the property that makes your work reproducible.
5. The query-API documentation (under construction, will live at `docs/api/`).

## If you are a backend or platform engineer

You want to extend Muninn — add an adapter, a feature, a service.

1. [README.md](../../README.md)
2. [AGENTS.md](../../AGENTS.md) — workflow contract.
3. [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md)
4. [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md) — module map.
5. [CODING_STANDARDS.md](CODING_STANDARDS.md) — what to type.
6. [TESTING_STRATEGY.md](TESTING_STRATEGY.md)
7. [TECH_STACK.md](TECH_STACK.md) — why each dependency exists.

## If you are an infrastructure or SRE engineer

You want to run Muninn, scale it, observe it, and recover it from failures.

1. [README.md](../../README.md)
2. [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md) — memory and deployment-profile contract.
3. [DATA_STORAGE_STRATEGY.md](DATA_STORAGE_STRATEGY.md) — where data lives.
4. [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md) — metrics, logs, traces, dashboards.
5. [TECH_STACK.md](TECH_STACK.md)
6. RUNBOOK (planned, not yet written) — operational playbooks.

## If you are a security reviewer or auditor

You want to understand trust boundaries, data handling, and the discipline behind reproducibility.

1. [SECURITY.md](../../SECURITY.md) — reporting and scope.
2. SECURITY_MODEL (planned) — threat model.
3. [DATA_STORAGE_STRATEGY.md](DATA_STORAGE_STRATEGY.md) — retention, deletion, recovery.
4. [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md) — audit and reproducibility properties.
5. [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md) — schema discipline, evolution rules.
6. [TESTING_STRATEGY.md](TESTING_STRATEGY.md) — the divergence checks that prove the discipline.

## If you are a reviewer or code-review participant

You want to know what "good" looks like in a Muninn PR.

1. [AI_AGENT_WORKFLOW.md](AI_AGENT_WORKFLOW.md) — the workflow that every PR should follow.
2. [CODING_STANDARDS.md](CODING_STANDARDS.md) — the line-level rules.
3. [TESTING_STRATEGY.md](TESTING_STRATEGY.md) — the test-layer expectations.
4. [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md) — the most common source of subtle review issues.
5. [NON_GOALS.md](NON_GOALS.md) — when to push back.

## If you are a new human contributor

You want to make your first contribution without breaking anything.

1. [README.md](../../README.md)
2. [CONTRIBUTING.md](../../CONTRIBUTING.md) — how to file, branch, commit, review.
3. [AGENTS.md](../../AGENTS.md) — workflow contract (applies to humans too).
4. [CODING_STANDARDS.md](CODING_STANDARDS.md)
5. [TESTING_STRATEGY.md](TESTING_STRATEGY.md)
6. [ROADMAP.md](ROADMAP.md) — find a phase-appropriate first task.

## If you are an AI coding agent

You want to make safe, architecturally consistent changes.

1. [AGENTS.md](../../AGENTS.md) — your contract.
2. [AI_AGENT_WORKFLOW.md](AI_AGENT_WORKFLOW.md) — your loop.
3. [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md)
4. [NON_GOALS.md](NON_GOALS.md)
5. [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md)
6. Whichever area-specific doc matches the task.

## If you are evaluating Muninn for adoption

You want to know whether this project is a fit for your use case.

1. [README.md](../../README.md)
2. [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md)
3. [NON_GOALS.md](NON_GOALS.md) — read this carefully; this is what we will not build.
4. [ROADMAP.md](ROADMAP.md) — what is built today, what is planned.
5. [TECH_STACK.md](TECH_STACK.md) — whether the stack aligns with yours.
6. [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md) — whether the footprint fits.

## Document Index

| Doc | One-line summary |
|---|---|
| [AGENTS.md](../../AGENTS.md) | Workflow contract for agents and contributors. |
| [CONTRIBUTING.md](../../CONTRIBUTING.md) | How to contribute as a human. |
| [SECURITY.md](../../SECURITY.md) | How to report vulnerabilities. |
| [CODE_OF_CONDUCT.md](../../CODE_OF_CONDUCT.md) | Behavioral standards. |
| [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md) | What Muninn is and why. |
| [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md) | Load-bearing principles. |
| [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md) | Hard local-runtime constraints. |
| [DOMAIN_MODEL.md](DOMAIN_MODEL.md) | Domain vocabulary. |
| [GLOSSARY.md](GLOSSARY.md) | A–Z lookup of terms. |
| [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md) | JSON now, Avro path. |
| [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md) | The most important doc. |
| [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md) | Module responsibilities. |
| [TECH_STACK.md](TECH_STACK.md) | Every dependency, justified. |
| [TESTING_STRATEGY.md](TESTING_STRATEGY.md) | Seven test layers. |
| [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md) | Logs, metrics, traces. |
| [DATA_STORAGE_STRATEGY.md](DATA_STORAGE_STRATEGY.md) | Where data lives. |
| [ROADMAP.md](ROADMAP.md) | Phased plan. |
| [AI_AGENT_WORKFLOW.md](AI_AGENT_WORKFLOW.md) | The agent loop. |
| [CODING_STANDARDS.md](CODING_STANDARDS.md) | What to type. |
| [NON_GOALS.md](NON_GOALS.md) | What we won't build. |
