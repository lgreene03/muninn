package io.muninn.architecture;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Random;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields;

/**
 * Architectural rules enforced at build time, drawn from:
 *
 * <ul>
 *   <li>{@code docs/steering/CODING_STANDARDS.md} — Forbidden Patterns.</li>
 *   <li>{@code docs/steering/DETERMINISTIC_REPLAY.md} — Anti-Patterns.</li>
 *   <li>{@code docs/steering/SERVICE_BOUNDARIES.md} — module boundaries.</li>
 * </ul>
 *
 * <p>Rules deliberately omitted (tracked for future enforcement):</p>
 * <ul>
 *   <li>{@code System.currentTimeMillis()} in feature code — currently reached via
 *       {@code UUIDv7.generate()} from {@code VwapComputer}; eventId determinism
 *       is a separate design question from computational determinism. Determinism
 *       tests cover the substantive output fields.</li>
 *   <li>Floating-point arithmetic in money-handling packages — needs a precise
 *       allowlist before enforcement.</li>
 * </ul>
 */
@AnalyzeClasses(
        packages = "io.muninn",
        importOptions = {ImportOption.DoNotIncludeTests.class}
)
class ArchitectureRulesTest {

    // ---- Determinism rules ---------------------------------------------------

    @ArchTest
    static final ArchRule no_wall_clock_in_feature_compute =
            noClasses()
                    .that().resideInAPackage("..feature.compute..")
                    .should().callMethod(Instant.class, "now")
                    .orShould().callMethod(System.class, "currentTimeMillis")
                    .orShould().callMethod(System.class, "nanoTime")
                    .because("feature computation must be a pure function of its inputs; "
                            + "see DETERMINISTIC_REPLAY.md §Anti-Patterns");

    @ArchTest
    static final ArchRule no_random_in_feature_code =
            noClasses()
                    .that().resideInAPackage("..feature..")
                    .should().callConstructor(Random.class)
                    .because("non-deterministic randomness in feature code breaks replay; "
                            + "if randomness is needed, the seed must be part of the FeatureDefinition");

    // ---- Layering rules ------------------------------------------------------

    @ArchTest
    static final ArchRule shared_module_has_no_internal_dependencies =
            noClasses()
                    .that().resideInAPackage("..shared..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "io.muninn.ingestion..",
                            "io.muninn.feature..",
                            "io.muninn.replay..",
                            "io.muninn.query..",
                            "io.muninn.stream..",
                            "io.muninn.streaming..",
                            "io.muninn.storage..",
                            "io.muninn.config..")
                    .because("shared-schema is a pure library; "
                            + "see SERVICE_BOUNDARIES.md §shared-schema");

    @ArchTest
    static final ArchRule feature_module_does_not_depend_on_ingestion =
            noClasses()
                    .that().resideInAPackage("..feature..")
                    .should().dependOnClassesThat().resideInAPackage("..ingestion..")
                    .because("feature-engine consumes from the event log, not from the ingestion service; "
                            + "see SERVICE_BOUNDARIES.md");

    @ArchTest
    static final ArchRule query_module_does_not_depend_on_feature =
            noClasses()
                    .that().resideInAPackage("..query..")
                    .should().dependOnClassesThat().resideInAPackage("..feature..")
                    .because("the query API reads persisted feature outputs via DuckDB; "
                            + "it does not invoke the feature engine; see SERVICE_BOUNDARIES.md");

    @ArchTest
    static final ArchRule query_api_depends_only_on_backend_abstraction =
            noClasses()
                    .that().resideInAPackage("io.muninn.query")
                    .and().areNotAssignableTo(io.muninn.query.backend.FeatureQueryBackend.class)
                    .should().dependOnClassesThat(
                            com.tngtech.archunit.base.DescribedPredicate.describe(
                                    "concrete FeatureQueryBackend implementations",
                                    clazz -> clazz.getName().equals(
                                            "io.muninn.query.backend.DuckDbFeatureQueryBackend")
                                            || clazz.getName().equals(
                                                    "io.muninn.query.backend.TrinoFeatureQueryBackend")))
                    .because("FeatureQueryController and FeatureQueryService must depend only "
                            + "on the FeatureQueryBackend abstraction; see ADR-0006");

    @ArchTest
    static final ArchRule streaming_does_not_depend_on_feature_ingestion_or_query =
            noClasses()
                    .that().resideInAPackage("..streaming..")
                    .should().dependOnClassesThat().resideInAnyPackage(
                            "io.muninn.feature..",
                            "io.muninn.ingestion..",
                            "io.muninn.query..")
                    .because("the SSE stream endpoint tails the feature output topics off Kafka; "
                            + "it does not invoke the feature engine, the ingestion service, or the "
                            + "warehouse query path; see ADR-0009");

    @ArchTest
    static final ArchRule ingestion_pipeline_depends_only_on_adapter_interface =
            noClasses()
                    .that().resideInAPackage("io.muninn.ingestion")
                    .and().doNotHaveSimpleName("IngestionAdapterConfiguration")
                    .should().dependOnClassesThat(
                            com.tngtech.archunit.base.DescribedPredicate.describe(
                                    "concrete ExchangeAdapter implementations",
                                    clazz -> clazz.getName().equals(
                                            "io.muninn.ingestion.adapter.BinanceWebSocketAdapter")
                                            || clazz.getName().equals(
                                                    "io.muninn.ingestion.adapter.CoinbaseExchangeAdapter")))
                    .because("IngestionPipeline must depend only on the ExchangeAdapter "
                            + "interface so adding a third exchange is a one-bean change; "
                            + "only IngestionAdapterConfiguration sees concrete adapters. "
                            + "See ADR-0008.");

    @ArchTest
    static final ArchRule archival_consumer_depends_only_on_sink_abstraction =
            noClasses()
                    .that().resideInAPackage("io.muninn.storage")
                    .and().doNotHaveSimpleName("FeatureSinkConfiguration")
                    .should().dependOnClassesThat(
                            com.tngtech.archunit.base.DescribedPredicate.describe(
                                    "concrete FeatureSink implementations",
                                    clazz -> clazz.getName().equals(
                                            "io.muninn.storage.sink.ParquetFeatureSink")
                                            || clazz.getName().equals(
                                                    "io.muninn.storage.sink.IcebergFeatureSink")))
                    .because("FeatureArchivalConsumer must depend only on the FeatureSink "
                            + "abstraction; only FeatureSinkConfiguration sees concrete sinks; "
                            + "see ADR-0007");

    // ---- Code hygiene rules --------------------------------------------------

    @ArchTest
    static final ArchRule no_field_injection =
            noFields()
                    .should().beAnnotatedWith(Autowired.class)
                    .because("constructor injection only; see CODING_STANDARDS.md §No Hidden Global State");

    @ArchTest
    static final ArchRule no_print_stack_trace =
            noClasses()
                    .should().callMethod(Throwable.class, "printStackTrace")
                    .because("use structured logging; see CODING_STANDARDS.md §Forbidden Patterns");
}
