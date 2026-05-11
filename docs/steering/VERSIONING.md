# VERSIONING.md

Muninn has three independent version axes. Confusing them is how compatibility breaks silently. This document defines each, the rules that govern it, and the conditions that constitute a "breaking change."

## The Three Axes

### 1. Application Version (semantic version)

The version of the Muninn codebase as a whole. Tagged in git, published on releases.

- **Format.** `MAJOR.MINOR.PATCH` per [semver.org](https://semver.org).
- **Pre-1.0.** Muninn is pre-1.0 in MVP. Minor bumps may include breaking changes; this is documented in CHANGELOG.
- **Post-1.0.** Strict semver:
  - `MAJOR` — incompatible API or schema changes.
  - `MINOR` — backwards-compatible additions.
  - `PATCH` — backwards-compatible fixes.

### 2. Event Schema Version

Per-event-type integer recorded as `schemaVersion` on every event. Governs the wire-and-storage contract between producers and consumers.

- **Format.** Monotonically increasing integer per event type. `1`, `2`, `3`, ...
- **Increment on.** Any change to the event's fields, types, or semantics.
- **Compatibility.** Consumers may switch on `schemaVersion` to handle multiple versions in the same stream. The event log retains events at all historical schema versions; rewriting history is forbidden.

### 3. Feature Version

The git SHA (or a semantic version string for stable releases) of the feature-engine code that produced a `FeatureComputedEvent`. Recorded on every output.

- **Format.** `<short-sha>` (e.g., `1a2b3c4`) in development; `<feature>@<semver>` (e.g., `vwap@1.2.0`) for released features.
- **Increment on.** Any change to the feature's computation logic, window definition, watermark policy, or output shape.
- **Compatibility.** Outputs from different feature versions are **not comparable**. Consumers explicitly choose a version. Replay can only be resumed from a checkpoint with a matching feature version ([DETERMINISTIC_REPLAY.md](DETERMINISTIC_REPLAY.md)).

## What Counts as "Breaking"

### For the application version

A change is breaking if any of:

- A public HTTP endpoint changes its URL, method, request shape, or response shape in an incompatible way.
- A configuration property is removed or has its default changed in a way that alters runtime behavior.
- A required dependency is bumped across its own major version.
- A `production-reference` deployment path requires migration work to adopt.

A change is **not** breaking if:

- A new endpoint is added.
- A new configuration property is added with a safe default.
- A dependency is upgraded within its own major version with no API change.
- Internal refactoring leaves public APIs intact.

### For event schema version

A change is breaking if any of:

- A field is removed.
- A field is renamed.
- A field's type changes (including widening, narrowing, or unit change).
- A field's semantic meaning changes (same name, different content).
- A required field is added (forbidden — see below).

A change is **not** breaking if:

- A new optional field is added with a sensible default.
- Documentation or comments are updated.
- The serialized representation is identical (e.g., Jackson annotation change with same output).

### For feature version

Any change to feature logic is treated as a version increment, regardless of whether outputs visibly change. The bar to bump is low because the consequences of *not* bumping (silent divergence) are high.

A change is breaking **always** when it changes feature outputs. That is the entire definition.

## Rules

### R1. Schema version is never reused

Once `TradeEvent.v1` exists in the log, `v1` is frozen. Changes go to `v2`.

### R2. Required fields are never added to an existing schema version

Adding a required field forces every old event to be re-emitted. This is forbidden. New fields are nullable; required-ness is enforced at the consumer or in a new schema version.

### R3. Field types are never changed

A `BigDecimal` price stays a `BigDecimal`. To change types, introduce a new field with a new name; deprecate the old.

### R4. Feature outputs are immutable per (feature, version, window)

If `vwap@1.0.0` emitted a value for the 14:00–14:01 window, that value is canonical for that combination forever. A bug fix produces `vwap@1.0.1` with a new value for the same window. Both exist in the log; consumers choose.

### R5. Replay must match its source's code version

A replay job is parameterized by `featureVersion`. The replay engine refuses to start with a feature version that doesn't match the requested checkpoint, or with a missing version.

### R6. Deprecation has a grace period

When a field is deprecated:

1. Mark it with `@Deprecated` and a comment naming the replacement.
2. Add a CHANGELOG entry.
3. Dual-write old and new fields for at least one minor version.
4. Remove the old field only after one full minor release with both present.

### R7. Migrations are immutable

Once a Flyway migration is in `main`, it is never edited. Schema changes go in new migrations. This is enforced by code review.

## CHANGELOG

The repository keeps a [CHANGELOG.md](../../CHANGELOG.md) in the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

Every release entry has sections:

- **Added.** New features.
- **Changed.** Behavior changes for existing features.
- **Deprecated.** Features marked for removal.
- **Removed.** Features deleted.
- **Fixed.** Bug fixes.
- **Security.** Vulnerability fixes.

PRs that ship a user-visible change include a CHANGELOG entry in the same PR.

## Release Process

(See `RELEASE_PROCESS.md` — planned.)

For MVP, releases are git tags on `main`:

1. CI must be green on `main`.
2. CHANGELOG has an `[Unreleased]` section ready to promote.
3. Bump `pom.xml` `<version>` to the release version.
4. Promote `[Unreleased]` → `[X.Y.Z] — YYYY-MM-DD` in CHANGELOG.
5. Commit: `chore: release X.Y.Z`.
6. Tag: `git tag -a vX.Y.Z -m "vX.Y.Z"`.
7. Push: `git push --follow-tags`.
8. GitHub Release with CHANGELOG section pasted as the body.

## Version Compatibility Matrix

This table will be filled in as releases ship. For each application version, it records:

- The event schema versions in active use.
- The feature versions in active use.
- Notes on incompatibilities with prior releases.

| App version | Event schemas | Feature versions | Notes |
|---|---|---|---|
| `0.1.0-SNAPSHOT` | `TradeEvent.v1`, `OrderBookSnapshotEvent.v1`, `CandleEvent.v1`, `FeatureComputedEvent.v1` | `vwap@dev` | Pre-1.0 — breaking changes possible without major bump |

## Anti-Patterns

- Editing a Flyway migration after it has run. **Forbidden.**
- Bumping a feature version "just to be safe" without a code change. **Forbidden** — versions must correspond to real code differences (a SHA is unique by definition; a semver-style version requires real change).
- Skipping a schema version (going from `v2` to `v4`). **Forbidden** — confuses tooling.
- Treating a SHA as semver. **Forbidden** — a SHA is not ordered by semantics.
- Soft-deleting events by writing a "deleted" flag. **Forbidden** — events are immutable; corrections are new events.

## When in Doubt

If you cannot decide whether a change is breaking, the answer is **yes, it is breaking** and you must bump the relevant version. False positives are cheap; false negatives are how systems quietly stop being trustworthy.
