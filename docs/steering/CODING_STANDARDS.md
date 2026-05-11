# CODING_STANDARDS.md

This document is concrete. It tells you what to type. Disagreement with a standard is resolved by changing the standard in a PR, not by ignoring it in code.

## Language

- **Java 21.** Use records, pattern matching, `switch` expressions, sealed types, and virtual threads where they clarify intent.
- **No Kotlin, Scala, or Groovy.** A single language across the codebase is a feature.
- **No preview features** in non-test code unless explicitly enabled in `pom.xml` and justified in a steering doc.

## Package Naming

```
io.muninn
├── shared                  // shared-schema module
│   ├── event               // Event records
│   ├── instrument          // Instrument, Exchange
│   ├── time                // Watermark, EventTime utilities
│   └── validation          // EventValidator and rules
├── ingestion               // ingestion-service module
│   ├── adapter             // exchange-specific adapters
│   ├── api                 // HTTP ingestion API
│   └── producer            // Kafka producer wiring
├── feature                 // feature-engine module
│   ├── engine              // core processor
│   ├── window              // window logic
│   ├── checkpoint          // checkpoint write/restore
│   └── definitions         // built-in features
├── replay                  // replay-engine module
│   ├── job                 // ReplayJob handling
│   ├── source              // event source implementations
│   └── divergence          // shadow-replay comparator
├── query                   // query-api module
│   ├── api                 // controllers
│   └── duckdb              // DuckDB query service
└── infra                   // cross-cutting (config, observability)
```

A class lives in the package that owns its responsibility, not in a "model" or "util" catch-all.

## Records and Immutability

- **Use `record` for every domain type.** No setters. No mutable fields.
- **Defensive copies in canonical constructors** for any collection or array field:
  ```java
  public record TradeEvent(..., List<Tag> tags) {
      public TradeEvent {
          tags = List.copyOf(tags);
      }
  }
  ```
- **No Lombok.** Records cover the use case without an annotation processor.
- **No JavaBean style** (`getFoo()`/`setFoo()`) for domain types.

## Explicit Time

- **`Instant`** for any absolute moment in time.
- **`Duration`** for any elapsed time.
- **`ZonedDateTime`** only where a calendar-aware time matters (display, exchange close hours).
- **Never `long`** as a timestamp without a unit suffix in the variable name (`epochMillis`, `epochSeconds`), and only at boundaries with external systems.
- **Never `LocalDateTime`** for event-time fields — it has no zone and silently drifts.
- **`Clock`** injected as a Spring bean. Never call `Instant.now()` from feature-engine code; call `clock.instant()`. Replay tests inject a fixed clock.

## No Hidden Global State

- **No `static` mutable fields.**
- **No singletons outside the Spring container.**
- **No service-locator patterns.** Wire dependencies via constructor injection.
- **No thread-local state** in feature-engine or replay-engine code. Pass state explicitly.

## Error Handling

- **Throw typed exceptions** from a small domain hierarchy:
  - `MuninnException` (root, runtime, abstract).
  - `ValidationException`, `SchemaException`, `ReplayException`, `IngestionException`, `StorageException` (subclasses).
- **Never catch `Exception`** at a service boundary unless the catch handler explicitly logs, increments a metric, and rethrows or wraps.
- **Never swallow exceptions.** A `catch (Exception ignored)` block is a defect.
- **Distinguish recoverable from fatal.** Recoverable errors (transient broker outage) → retry with backoff. Fatal errors (schema mismatch) → fail fast and surface in metrics.
- **Avoid checked exceptions** for new code. Wrap and rethrow as a domain runtime exception at the boundary.

## Structured Logging

- **SLF4J + Logback** with `logstash-logback-encoder` for JSON output.
- **Use key/value pairs via MDC or the SLF4J fluent API.** Never concatenate context into the message:
  ```java
  // bad
  log.info("Processed event " + eventId + " for " + instrument);

  // good
  log.atInfo()
     .addKeyValue("eventId", eventId)
     .addKeyValue("instrument", instrument)
     .log("Event processed");
  ```
- **Log levels** as defined in [OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md).
- **Never log secrets** (API keys, tokens, PII). A `@Sensitive` annotation marks fields whose `toString()` returns `***`.

## Configuration

- **Spring `@ConfigurationProperties`** classes for every config block. No `@Value("${...}")` scattered across services.
- **Profile-driven**:
  - `local-lite` — single process, in-memory where viable.
  - `local-full` — full local stack with observability.
  - `cloud-cheap` — single VPS, free-tier services.
  - `production-reference` — scaled deployment.
- **Defaults in `application.yml`**; profile overrides in `application-<profile>.yml`.
- **Secrets** via environment variables only, never committed.

## Testing

- **Test class name** = `<UnitUnderTest>Test`. One class per unit.
- **Test method name** = `methodOrScenario_state_expectedBehavior`. Example: `validate_blankSource_throwsValidationException`.
- **Arrange / Act / Assert** structure, separated by blank lines.
- **AssertJ for assertions.** `assertThat(...).is...` reads more naturally than JUnit's bare `assertEquals`.
- **Mockito for mocks**, sparingly. Prefer real objects with test doubles only at infrastructure boundaries.
- **Testcontainers for integration tests.** No embedded brokers, no in-memory PostgreSQL.

See [TESTING_STRATEGY.md](TESTING_STRATEGY.md) for the layered test plan.

## Documentation in Code

- **`package-info.java`** for every public package: one paragraph stating the package's responsibility, what it owns, what it must not do.
- **Javadoc on public types and methods** when the name does not fully convey intent.
- **No commented-out code.** Delete it; git remembers.
- **No comments that restate the code.** `// increment counter` above `counter++` is noise.
- **Comments explain *why*.** Hidden constraints, non-obvious decisions, links to the steering doc that drove the choice.

## Build Hygiene

- **`mvn verify` is clean** — no warnings, no test failures, no Spotless violations.
- **ArchUnit rules** enforce package boundaries (no `feature` → `ingestion`, no `query` → `feature`, etc.).
- **Spotless** enforces Google Java Format with project-specific overrides.
- **OWASP dependency check** runs in CI; high-severity findings fail the build.

## Dependencies

- **Adding a dependency requires** a one-line entry in [TECH_STACK.md](TECH_STACK.md) explaining why.
- **Pinned versions** via `<dependencyManagement>` or Maven BOMs. No version ranges.
- **No unmaintained libraries.** Last release > 2 years ago is a red flag.

## Forbidden Patterns

Static-analysis rules fail the build on:

- `Instant.now()`, `System.currentTimeMillis()`, `LocalDateTime.now()`, `new Date()` inside `feature.*` packages.
- `new Random()` without a seed.
- `@Autowired` on fields. Use constructor injection.
- `@SuppressWarnings("all")`.
- `printStackTrace()`.
- `Thread.sleep()` outside of test fixtures.

## What "Done" Means

A change is done when:

1. It compiles cleanly.
2. All tests pass, including the relevant determinism layer.
3. `./scripts/smoke.sh` passes if the change touches the runtime path.
4. Documentation is updated.
5. The PR summary describes what was tested.

Nothing less.
