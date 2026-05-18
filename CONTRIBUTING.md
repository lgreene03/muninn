# Contributing to Muninn

Thank you for considering a contribution. Muninn is a serious infrastructure project built in public, and contributions — code, documentation, bug reports, design feedback — are welcome.

Before opening a PR, please read:

1. [README.md](README.md) — project overview.
2. [AGENTS.md](AGENTS.md) — the workflow contract (applies to humans too).
3. [docs/steering/ARCHITECTURE_PRINCIPLES.md](docs/steering/ARCHITECTURE_PRINCIPLES.md)
4. [docs/steering/NON_GOALS.md](docs/steering/NON_GOALS.md)

If your change conflicts with any steering doc, **surface the conflict first** — open an issue or a draft PR with the proposed doc change. Do not silently override the architecture.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By participating you agree to uphold it.

## Ways to Contribute

- **Bug reports.** Use the issue template. Include reproduction steps, observed vs expected behavior, environment, and logs if relevant.
- **Feature requests.** Check [NON_GOALS.md](docs/steering/NON_GOALS.md) first. If still relevant, open an issue describing the use case before writing code.
- **Documentation.** Doc-only PRs are welcome and reviewed quickly. Keeping steering docs accurate is load-bearing.
- **Code.** See "Code Contributions" below.

## Getting Started

```bash
git clone https://github.com/lgreene03/muninn.git
cd muninn

# Bring up the complete local telemetry and infrastructure stack
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d

# Run formatting checks and apply standard Spotless layout
mvn spotless:apply

# Run the full test suite
mvn verify

# Run the E2E verification smoke test
./scripts/smoke.sh
```

Java 21 and Docker are required. A Mac mini M4 with 24 GB RAM is the reference machine; any comparable Linux/macOS workstation works.

## Code Contributions

### 1. Open an issue first

For anything larger than a typo fix, open an issue describing:

- The problem.
- The proposed approach.
- Which steering docs apply.
- Which test layers will cover it.

This avoids wasted work and ensures the change fits the architecture.

### 2. Branch and commit

- Branch from `main`. Naming: `feature/short-description`, `fix/short-description`, `docs/short-description`, `chore/short-description`.
- Small commits, one logical change each. Commit messages explain *why*, not *what*.
- Use Conventional Commits where natural: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
- Sign off your commits if you wish — not required.

### 3. Follow the workflow

The loop from [AI_AGENT_WORKFLOW.md](docs/steering/AI_AGENT_WORKFLOW.md) applies to humans too:

```
READ → PLAN → TEST → CODE → DOC → SUMMARIZE
```

Tests are written before or with the code, not after. Documentation is updated **in the same commit** as the code it describes.

### 4. Quality bar

A PR is mergeable when:

- It compiles cleanly with no warnings.
- All tests pass, including the relevant determinism layer ([TESTING_STRATEGY.md](docs/steering/TESTING_STRATEGY.md)).
- `./scripts/smoke.sh` passes if the runtime path is touched.
- `mvn spotless:check` passes (run `mvn spotless:apply` to fix).
- ArchUnit rules pass.
- Documentation is updated.
- The PR description follows the template.

### 5. PR description

Use the [pull request template](.github/PULL_REQUEST_TEMPLATE.md). It asks for:

- What changed.
- Why.
- How it was tested.
- What was deliberately not done.
- Open questions.

A PR that says "fixes things" without specifying what was tested will be returned.

## Coding Standards

See [docs/steering/CODING_STANDARDS.md](docs/steering/CODING_STANDARDS.md) for the full set. Highlights:

- Java 21 records for domain types.
- Constructor injection only.
- Explicit time types (`Instant`, `Duration`).
- Structured logging via SLF4J fluent API or MDC.
- No `Instant.now()` in feature-engine code; inject `Clock`.
- No new dependency without a one-line entry in [TECH_STACK.md](docs/steering/TECH_STACK.md).

## Testing Expectations

See [docs/steering/TESTING_STRATEGY.md](docs/steering/TESTING_STRATEGY.md). At minimum:

- New code has unit tests.
- Changes to feature computation have determinism tests.
- Changes to schemas have contract tests with updated golden files.
- Changes touching brokers, databases, or object storage have Testcontainers integration tests.

If a change cannot be tested in this repository, say so explicitly in the PR.

## AI-Assisted Contributions

Muninn is built with AI coding agents as first-class contributors. If you used an AI assistant (Claude Code, Cursor, Copilot, etc.):

- You are responsible for the correctness of the code, regardless of how it was produced.
- The PR description should mention AI assistance briefly — not for blame, but for accuracy.
- The contribution is licensed under Apache 2.0 like any other.
- All rules in [AGENTS.md](AGENTS.md) and [AI_AGENT_WORKFLOW.md](docs/steering/AI_AGENT_WORKFLOW.md) apply.

## Reporting Security Issues

Do **not** open a public issue for security vulnerabilities. See [SECURITY.md](SECURITY.md) for the private reporting process.

## Licensing

By contributing, you agree that your contribution is licensed under the [Apache License 2.0](LICENSE). The project does not require a separate CLA.

## Questions

Open a [Discussion](https://github.com/lgreene03/muninn/discussions) or an issue with the `question` label. For substantive architectural questions, draft a steering-doc update as the start of the conversation.
