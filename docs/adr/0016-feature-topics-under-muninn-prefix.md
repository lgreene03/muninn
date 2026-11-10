# 0016. muninn's feature topics live under `muninn.features.`

- **Status:** Accepted
- **Date:** 2026-11-10
- **Deciders:** Project owner (norse-stack decision D1, `docs/DECISIONS.md`); feature-engine maintainers
- **Related:** amends the topic naming in [ADR-0009](0009-streaming-features-sse.md) and [ADR-0015](0015-generalized-feature-dispatch.md); norse-stack goal G0.10

## Context

`FeatureComputedEvent#topicName()` returned `features.<name>.<version>`, so muninn's
order-book imbalance went to `features.obi.v1`. In norse-stack that topic already belongs
to obi-bridge, and huginn trades on it. The two producers write different records under
one name: if muninn's feature engine ran in the stack, its OBI would land in huginn's
trading input, and muninn's own pattern subscriptions (shadow comparator, live stream)
would read obi-bridge's records, none of which is a `FeatureComputedEvent`. norse-stack
had to switch muninn's engine, comparator and stream off to stay safe.

## Decision

muninn publishes every feature to `muninn.features.<name>.<version>`, and every replay
output to that topic's `.replay` sibling. Its own subscriptions read only that namespace.
The bare `features.` namespace is not muninn's; `features.obi.v1` stays obi-bridge's,
frozen.

The prefix is the smallest change that removes the collision: one constant
(`FeatureComputedEvent.TOPIC_PREFIX`), the comparator and stream patterns, the archival
listener, and the replay job's reported output sink. muninn's OBI is retired as an alpha,
and huginn already reads obi-bridge's format, so nothing downstream has to move.

## Consequences

- muninn's feature engine, shadow comparator and live stream can run in norse-stack
  without any route into huginn's trading input.
- `FeatureEngineRunner` refuses to publish to any topic outside the prefix, whatever topic
  resolver it was built with, and `FeatureTopicNamespaceTest` fails if any producer path
  (live or replay, all four computers) or subscription leaves it. The parity integration
  test also asks the real broker that no bare `features.*` topic was created.
- Topic names change for any external consumer of muninn's features. No Norse service is
  configured to read them today: every huginn config reads `features.obi.v1`, which is
  obi-bridge's. If a muninn feature ever earns a place in trading, huginn is migrated to
  it deliberately then. Deployments that provision topics by hand (norse-stack's
  `provision-topics.sh`) must provision the new names before enabling the engine.
- The archival listener now subscribes to the topic VWAP is actually published to. It had
  named `features.vwap.v1`, which no producer wrote (VWAP's name is `vwap.1m`), so it never
  archived anything; with the engine on it will now write Parquet to the warehouse bucket.
- Archive object paths (`features.<name>.<version>/instrument=...`) are storage layout in
  muninn's own bucket, not Kafka topics, and are unchanged.

## Alternatives Considered

- **muninn keeps `features.`, obi-bridge moves.** Rejected by the owner: huginn already
  reads obi-bridge's topic and format, so this moves the trading input to protect a retired
  research feature.
- **Rename only `obi`.** Leaves every other bare name open to the same collision the next
  time another system publishes a feature.
- **Status quo, with muninn's engine off in norse-stack.** Safe only while someone
  remembers the switch; it also keeps the engine, comparator and stream out of the stack.

## References

- norse-stack `docs/DECISIONS.md`, D1 (2026-11-10).
- norse-stack `docs/ITERATION_GOALS.md`, G0.10.
