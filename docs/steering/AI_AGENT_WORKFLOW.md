# AI_AGENT_WORKFLOW.md

This document is the operating manual for AI coding agents working on Muninn. It complements [AGENTS.md](../../AGENTS.md), which is the high-level contract. This document is the loop.

## The Loop

Every change, large or small, follows the same loop:

```
1. READ  → 2. PLAN  → 3. TEST  → 4. CODE  → 5. DOC  → 6. SUMMARIZE
```

Skipping a step is a defect.

---

## 1. Read

Before any edit, read the steering documents relevant to the change. The agent should be able to name them in its plan.

**Always.**
- [AGENTS.md](../../AGENTS.md)
- [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md)
- [NON_GOALS.md](NON_GOALS.md)

**By area.**
- Feature engine changes → [DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md), [DOMAIN_MODEL.md](DOMAIN_MODEL.md).
- Schema changes → [EVENT_SCHEMA_STRATEGY.md](EVENT_SCHEMA_STRATEGY.md).
- Service additions → [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md).
- Storage changes → [DATA_STORAGE_STRATEGY.md](DATA_STORAGE_STRATEGY.md).
- Tooling / stack questions → [TECH_STACK.md](TECH_STACK.md).
- Anything cross-cutting → [LOCAL_FIRST_CONSTRAINTS.md](LOCAL_FIRST_CONSTRAINTS.md).

If a doc is missing the relevant guidance, **stop and write it before coding**. An undocumented architectural decision is worse than no decision.

---

## 2. Plan

Write the plan **before** editing code. The plan states:

1. **Goal.** One sentence. What property of the system will be true after this change?
2. **Scope.** Which modules, files, packages. Which steering docs apply.
3. **Approach.** The chosen design, briefly. Alternatives considered, briefly.
4. **Tests.** Which test layers will cover this ([TESTING_STRATEGY.md](TESTING_STRATEGY.md)). What new test data, if any.
5. **Risks.** What could go wrong. How will it be detected.
6. **Doc updates.** Which steering docs change, and how.

For small changes, the plan is three lines. For architectural changes, it is a page. Either way, it precedes the diff.

---

## 3. Test

Tests are written **before or alongside** the code, not after. The order is:

1. Write a failing test that describes the desired behavior.
2. Write the minimum code to pass it.
3. Refactor with tests green.

This is non-negotiable for `feature-engine`, `shared-schema`, and replay-related code. For trivial wiring changes, a single integration test suffices.

If a change cannot be tested in this repository's test infrastructure, the agent must say so explicitly in the summary. **Do not claim a change works without testing it.**

---

## 4. Code

- **Small commits.** One logical change per commit. The commit message explains *why*, not *what*.
- **No broad rewrites.** Refactor incrementally. A 500-line diff that "cleans up" three modules is a defect.
- **No silent format changes.** Run `mvn spotless:apply` separately if needed; never bundle formatting and logic in the same commit.
- **No drive-by edits.** If you notice an unrelated bug, file an issue or add a TODO with an issue number. Do not fix it in the same change.
- **No unnecessary abstraction.** YAGNI applies aggressively. An interface with one implementation is a smell unless it is a service boundary defined in [SERVICE_BOUNDARIES.md](SERVICE_BOUNDARIES.md).

---

## 5. Doc

Update documentation in the **same commit** as the code change, not later:

- Architectural decisions → update the relevant steering doc.
- New public APIs → update OpenAPI spec and `package-info.java`.
- New metrics → update [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md).
- New configuration → update `application.yml` defaults and the relevant doc.
- New invariants → add to [ARCHITECTURE_PRINCIPLES.md](ARCHITECTURE_PRINCIPLES.md) if cross-cutting, or to the local module's doc otherwise.

**Stale docs are worse than no docs.** If you cannot keep them in sync, do not make the change.

---

## 6. Summarize

Every PR (or change in a long-running session) ends with a summary that contains:

1. **What changed.** Files, modules, public-API surface.
2. **Why.** The user-visible or system-level property gained.
3. **How it was tested.** Which test layers, which scenarios.
4. **What was not done.** Known follow-ups, deferred items, deliberate non-goals.
5. **Open questions.** Things the next agent or reviewer should decide.

A summary that says "everything works" without specifying what was tested is **not acceptable**.

---

## Architectural Consistency

The agent must check, in every PR:

- Does this preserve the **one computation path** for live and replay?
- Does this respect the **local-first** memory and footprint budget?
- Does this leave the system **observable** — new code emits the relevant telemetry?
- Does this respect **immutability** of events and the **append-only** property of the log?
- Have **non-goals** been respected?

A "yes" to all five is the bar. A "no" to any requires a written justification in the PR.

---

## What Agents Must NOT Do

- Run `git reset --hard`, `git push --force`, `rm -rf`, `docker volume rm`, or any destructive command without explicit human approval in the chat.
- Modify CI configuration without explicit human approval.
- Add a new dependency without checking it against [TECH_STACK.md](TECH_STACK.md) and recording it in the doc.
- Claim a change works without running the tests.
- Skip the doc step "for now."
- Disable, skip, or quarantine a failing determinism test.

---

## Working With Humans

- When a human asks for "a quick fix," ask whether tests are required. The answer is almost always yes.
- When a human asks for "just X," check whether X aligns with the steering docs. If not, surface the conflict and propose a path.
- When uncertain about an architectural choice, write a draft of the relevant steering doc update and ask for review before coding.

The agent's job is not to maximize lines of code shipped. It is to maximize the rate at which Muninn becomes the system it is designed to be.
