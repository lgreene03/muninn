# 0002. `eventId` is provenance metadata, not computational output

- **Status:** Accepted
- **Date:** 2026-05-12
- **Deciders:** Project maintainer
- **Related:** [DETERMINISTIC_REPLAY.md](../steering/DETERMINISTIC_REPLAY.md), [VERSIONING.md](../steering/VERSIONING.md), [ARCHITECTURE_PRINCIPLES.md](../steering/ARCHITECTURE_PRINCIPLES.md)

## Context

[DETERMINISTIC_REPLAY.md](../steering/DETERMINISTIC_REPLAY.md) states that the feature engine must produce byte-identical outputs given the same input sequence. The current implementation of `VwapComputer` constructs each `FeatureComputedEvent` with an `eventId` generated via `UUIDv7.generate()`, which internally reads `System.currentTimeMillis()`.

Reading the wall clock makes each generated `eventId` different across runs of the same input. Strictly interpreted, this violates byte-identical determinism.

There are two readings of "deterministic":

1. **Strict.** Every byte of the output is a function of the input. Under this reading, `eventId` must be derived from the input (e.g., a deterministic hash of `featureName`, `featureVersion`, `windowStart`, and `inputEventIds`).
2. **Computational.** The values the system reasons about — the computed quantity, the window boundaries, the inputs cited as provenance — are byte-identical. Identifiers attached for tracing and observability may legitimately differ, because they are not part of the computational claim.

The strict reading is more defensible. The computational reading is what most event-sourced systems in practice deliver, because identifiers are usually treated as opaque trace tokens.

ArchUnit currently forbids wall-clock reads inside `feature.compute.*` (see [ArchitectureRulesTest](../../src/test/java/io/muninn/architecture/ArchitectureRulesTest.java)), but the rule does not transitively catch wall-clock reads reached through `UUIDv7.generate()`. The rule's narrowness is deliberate — broadening it would require an alternative for `eventId` generation.

## Decision

For Muninn's MVP, `eventId` is treated as **provenance metadata**, not as a computational output:

- The feature engine's determinism claim covers `featureName`, `featureVersion`, `codeVersion`, `windowStart`, `windowEnd`, `value`/`values`, and the set of `inputEventIds`.
- The `eventId` of a `FeatureComputedEvent` is allowed to differ across runs.
- The [`ReplayDivergenceDetector`](../../src/main/java/io/muninn/replay/ReplayDivergenceDetector.java) compares the computational fields and ignores `eventId`.
- The integration test [`ReplayDeterminismIntegrationTest`](../../src/test/java/io/muninn/replay/ReplayDeterminismIntegrationTest.java) asserts equality on the computational fields and explicitly excludes `eventId`.

This is a pragmatic choice that preserves the architectural claim that matters (downstream consumers see the same values for the same inputs) while keeping the implementation simple.

## Consequences

**Easier.** UUIDv7 remains usable as the universal identifier scheme. No need for a deterministic-ID generation strategy that depends on hashing arbitrary input sets. ArchUnit rules can stay narrow and not flag every transitive wall-clock read.

**Harder.** Anyone diffing raw event payloads byte-for-byte between live and replay will see `eventId` differences. The divergence detector and the determinism test must explicitly compare the right fields. Documentation must surface this clearly so the property is not misrepresented.

**Tradeoff.** A future audit-or-compliance scenario could require strict byte-identity. If that arises, we revisit by:
- Deriving `eventId` from a hash of `(featureName, featureVersion, codeVersion, windowStart, windowEnd, sortedInputEventIds)`.
- Extending the ArchUnit rule to forbid `System.currentTimeMillis` transitively in `feature.compute.*`.
- Replacing the integration test's field-by-field comparison with a full bytewise equality check.

## Alternatives Considered

- **Strict bytewise determinism with derived `eventId`.** Rejected for MVP. Adds complexity (the hashing scheme is itself a versioned API), and the property it preserves is not load-bearing for any current consumer.
- **Pass `eventId` in through the `WindowedBatch`.** Rejected. Moves the wall-clock read up the call stack without solving it; would require the orchestrator to read the clock instead, which is the same problem in a different package.
- **Inject a `Clock` into `UUIDv7.generate(Clock)`.** Plausible refinement. Tests could pass a fixed clock; production would pass a system clock. Does not solve cross-run identity (two production runs still differ), but does make tests fully deterministic. Considered for a follow-up.
- **Use `Random` with a seed in the feature definition.** Rejected. Adds a seed surface to every feature for no current benefit, and the seed itself would need to be part of the replay manifest.

## References

- [`VwapComputer.compute()`](../../src/main/java/io/muninn/feature/compute/VwapComputer.java) — site of the `UUIDv7.generate()` call.
- [`UUIDv7`](../../src/main/java/io/muninn/shared/time/UUIDv7.java) — current generator.
- [`ArchitectureRulesTest`](../../src/test/java/io/muninn/architecture/ArchitectureRulesTest.java) — the scoped rule and its documented deferral.
- RFC 9562 (UUIDv7 specification).
