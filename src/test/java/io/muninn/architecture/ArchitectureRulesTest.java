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
