# 0008. Multi-exchange adapter framework

- **Status:** Accepted
- **Date:** 2026-05-18
- **Deciders:** Project maintainer
- **Related:** [ADR-0006 — Trino query backend](0006-trino-query-backend.md), [ADR-0007 — Iceberg feature sink](0007-iceberg-feature-sink.md), [SERVICE_BOUNDARIES.md](../steering/SERVICE_BOUNDARIES.md), [DOMAIN_MODEL.md](../steering/DOMAIN_MODEL.md)

## Context

Phase 1 shipped a single exchange adapter for Binance. The `ExchangeAdapter` interface existed from day one, but `IngestionPipeline.onApplicationReady()` `new`'d a `BinanceWebSocketAdapter` inline, hardcoded `binance.spot.v1` into every metric tag, and held a single `lastEventTime` field. Adding a second source meant editing the pipeline itself.

Phase 8's exit criterion calls for "a multi-exchange adapter framework". The remaining Phase 8 application work that wasn't already addressed by ADR-0006 (read path) and ADR-0007 (write path) was exactly this — making "add a second exchange" a self-contained change that doesn't touch core pipeline code.

Two approaches considered:

1. **Add per-exchange branches.** Extend `IngestionPipeline` with `if (binanceConfig.enabled()) { ... } if (coinbaseConfig.enabled()) { ... }`. Cheap up front; expensive every time a new source is added; couples the pipeline to every adapter's lifecycle.
2. **Inject a collection of `ExchangeAdapter`s.** The pipeline iterates the list Spring wired. Each adapter is a separately-configured bean with its own `@ConditionalOnProperty` enable flag. Adding a third exchange is a one-bean-method change.

Approach (2) is symmetric with the patterns ADR-0006 and ADR-0007 already established (sealed list of implementations behind an interface, per-implementation Spring config, profile-driven enablement). Picking it here keeps the codebase's mental model consistent.

## Decision

`IngestionPipeline` now accepts a `List<ExchangeAdapter>` injected by Spring. The list contains exactly the adapter beans whose corresponding `muninn.ingestion.<name>.enabled` flag is true at startup. On `ApplicationReadyEvent` the pipeline starts every adapter. All adapters deliver to the same `handleEvent` callback, which reads the event's `source()` field to drive per-source metric tagging.

`IngestionAdapterConfiguration` is the one place concrete adapters are constructed. Two beans today — Binance and Coinbase — each gated by `@ConditionalOnProperty`. The class is the only file in `io.muninn.ingestion.*` that references concrete adapter classes; an ArchUnit rule (`ingestion_pipeline_depends_only_on_adapter_interface` in `ArchitectureRulesTest`) enforces that.

### Per-source state

Three things were single-valued and need to be per-source after this change:

- **Metric tags.** The four ingestion metrics (`muninn.ingest.events.total`, `validation.failed`, `source.latency`, `lag.seconds`) all carry a `source` tag matching the adapter that emitted the event. They're lazily registered the first time an adapter starts.
- **Lag clock.** A `ConcurrentMap<String, Instant>` tracks `lastEventTime` per source. The lag gauge reads its source's entry on each scrape.
- **Counter and timer instances.** Held in `ConcurrentMap<String, Counter|Timer>` so the handler can dispatch by source without re-querying the registry on the hot path.

Cross-source state is intentionally absent. The handler is shared but each call reads the event's own `source()` field, so concurrent dispatch from multiple adapters never reads or writes another source's bucket.

### Configuration shape

```yaml
muninn:
  ingestion:
    binance:
      enabled: true     # default — Phase 1 behaviour preserved
      base-url: wss://stream.binance.com:9443
      instruments: [btcusdt]
      streams: [trade, "depth20@100ms"]
    coinbase:
      enabled: false    # default — opt-in second source
      base-url: wss://ws-feed.exchange.coinbase.com
      instruments: [BTC-USD]
      channels: [matches, level2]
```

The Helm `deploy/helm/muninn/templates/deployment-ingestion.yaml` template wires both blocks to `MUNINN_INGESTION_*` env vars; values not used by the chosen adapter are simply unread.

### How to add a third exchange

Documented inline in `IngestionAdapterConfiguration` (and reproduced here):

1. Add a `<Name>Config` record with `@ConfigurationProperties(prefix = "muninn.ingestion.<name>")`.
2. Add a `<Name>ExchangeAdapter` implementing `ExchangeAdapter`. Define its source string (e.g., `"kraken.spot.v1"`) and emit canonical `MarketEvent`s.
3. Add a `@Bean @ConditionalOnProperty(name = "muninn.ingestion.<name>.enabled", havingValue = "true")` method in `IngestionAdapterConfiguration`.
4. Add a row to `V003__seed_reference_data.sql` for the exchange's reference-data entry.
5. Document the source identifier in `DOMAIN_MODEL.md §Exchange`.
6. Update `deploy/helm/muninn/values.yaml` with a default `ingestion.<name>.enabled: false`.

No changes to `IngestionPipeline`, no changes to other adapters, no migration of existing topics or data. The ArchUnit rule prevents accidental imports of the new concrete adapter from anywhere except the configuration class.

## Consequences

**Easier.**

- "Add Kraken" is a self-contained 5-file PR with no risk of breaking the Binance path.
- Per-source dashboards work without per-source code. Once a new adapter is registered, its metrics appear automatically with the right tags.
- Disabling an adapter at runtime is a Helm values flip + a pod restart; no rebuild.
- The `ExchangeAdapter` interface contract — `start(handler)`, `stop()`, `source()` — is small enough that test doubles (like the multi-source pipeline test's `RecordingAdapter`) take ~10 lines.

**Harder / cost.**

- Two adapters means two configurations to keep consistent. Reference data (in `V003__seed_reference_data.sql`) must be updated whenever a new source is added; if it isn't, validation rejects events with an unknown instrument.
- Per-source metric tags inflate the Prometheus cardinality slightly. With one or two exchanges this is invisible; if the project ever supported dozens, this would need revisiting.
- Cross-exchange features (e.g., a spread between Binance and Coinbase BTC-USD) aren't part of this framework. They'd need a downstream join service. Out of scope.

**Operational.**

- The default Helm values flip Coinbase off (`enabled: false`); operators opt in. Binance stays on by default to preserve Phase 1 behaviour.
- When Coinbase is enabled, the dashboards' `source` filter shows both. The Determinism panel's divergence counter is per-feature, not per-source, and remains a global property of the feature engine.

## Alternatives Considered

- **`if (binanceConfig.enabled) ...` in the pipeline.** Rejected: cost of adding a third exchange grows with each branch; the pipeline file becomes the place where every exchange's lifecycle assumptions concentrate.
- **A registry pattern.** Adapters register themselves with a singleton at startup. Rejected: relies on side-effects in static init or `@PostConstruct`, harder to test, no compile-time list of active adapters.
- **One adapter per JVM process.** Run a separate Muninn instance per exchange. Considered. Higher operational cost (more pods, more service-discovery, more Kafka consumer groups), no real isolation benefit because the validation and metric layers can already shard by source. Rejected for Phase 8; could be revisited at scale.

## References

- `src/main/java/io/muninn/ingestion/adapter/ExchangeAdapter.java` — the interface.
- `src/main/java/io/muninn/ingestion/adapter/IngestionAdapterConfiguration.java` — the one place concrete adapters are constructed.
- `src/main/java/io/muninn/ingestion/IngestionPipeline.java` — the consumer.
- `src/test/java/io/muninn/ingestion/IngestionPipelineMultiSourceTest.java` — verifies multi-source dispatch and per-source metrics.
- `src/test/java/io/muninn/architecture/ArchitectureRulesTest.java::ingestion_pipeline_depends_only_on_adapter_interface` — the enforced boundary.
