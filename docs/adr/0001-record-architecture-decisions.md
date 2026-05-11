# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-05-11
- **Deciders:** Project maintainer
- **Related:** [AGENTS.md](../../AGENTS.md), [AI_AGENT_WORKFLOW.md](../steering/AI_AGENT_WORKFLOW.md), [ROADMAP.md](../steering/ROADMAP.md)

## Context

Muninn has a comprehensive set of [steering documents](../steering/) that describe how the system *is* — its principles, constraints, vocabulary, and rules. Steering docs are evergreen: they are updated as the system evolves and they describe the current state of the world.

Steering docs do not, by themselves, record *why* the world is that way. When a future contributor asks "why did we choose JSON over Avro?", or "why is the feature engine a single process?", the steering doc says *what* we chose, not *what we considered and why we rejected the alternatives*.

That history is valuable. Without it, future contributors re-litigate decisions that were already made, or worse, reverse them without understanding what they cost.

## Decision

Muninn will record significant architectural decisions as **Architecture Decision Records (ADRs)** in [`docs/adr/`](.). Each ADR is a short Markdown file following the template in [`0000-template.md`](0000-template.md).

ADRs are:

- **Numbered sequentially.** Filename: `NNNN-short-title.md`.
- **Immutable in spirit.** Once accepted, an ADR is not edited except for typos, status changes, or links to superseding ADRs.
- **Superseded, not deleted.** A reversed decision results in a new ADR that supersedes the old one. Both remain in the repository.
- **Concise.** One to two pages. If longer, the decision is probably two decisions.

An ADR is required when a change:

- Adds, removes, or replaces a load-bearing dependency.
- Introduces or removes a service boundary.
- Changes a property documented as load-bearing in [ARCHITECTURE_PRINCIPLES.md](../steering/ARCHITECTURE_PRINCIPLES.md).
- Reverses a previous ADR.

An ADR is **not** required for routine implementation choices, bug fixes, or doc updates.

## Consequences

**Easier.** Future contributors can reconstruct the reasoning behind the system as it stands. New AI agents can read the ADR set as part of onboarding. Reversing a decision becomes a deliberate act with a paper trail.

**Harder.** Every significant decision now requires a written record. This adds a small amount of friction to architectural change — which is the intended effect.

**Obligations.** The maintainer is responsible for ensuring an ADR exists before merging a change that meets the criteria above. The PR template prompts for this.

**Relationship to steering docs.** Steering docs describe the *current* system. ADRs describe *decisions and their reasoning at a moment in time*. When an ADR is accepted, the relevant steering doc is updated **in the same PR**. The ADR points to the doc it changed; the doc may reference the ADR.

## Alternatives Considered

- **Git commit messages only.** Rejected. Commit messages are not discoverable; nobody searches commit history to learn the architecture.
- **Wiki or external doc site.** Rejected. Documentation that lives outside the repository drifts. Keeping ADRs alongside code ensures they are versioned, reviewed, and survive a `git clone`.
- **Steering docs as the sole record.** Rejected. Steering docs describe *what is*. They lose the texture of *what was considered*.
- **No ADRs.** Rejected. The steering doc set is large; without ADRs, the reasons behind individual choices erode.

## References

- Michael Nygard, ["Documenting Architecture Decisions"](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions) (2011).
- [adr.github.io](https://adr.github.io/) — community conventions.
- [`0000-template.md`](0000-template.md) — the template every ADR uses.
