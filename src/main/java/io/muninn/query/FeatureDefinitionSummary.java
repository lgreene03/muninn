package io.muninn.query;

/**
 * API view of a registered feature definition, returned by
 * {@code GET /api/v1/features}.
 *
 * <p>This is the query module's own DTO, deliberately decoupled from the
 * feature-engine's internal {@code io.muninn.feature.FeatureDefinition} record
 * — the query module must not depend on {@code io.muninn.feature..} (see
 * {@code ArchitectureRulesTest.query_module_does_not_depend_on_feature}). The
 * camelCase field names match the published OpenAPI contract and the
 * muninn-py SDK's {@code FeatureDefinition} model.</p>
 *
 * @param name           feature name, e.g. {@code "vwap.1m"}
 * @param version        schema version, e.g. {@code "v1"}
 * @param description    human-readable description, may be {@code null}
 * @param outputKind     aggregation kind, e.g. {@code "VWAP"}, may be {@code null}
 * @param windowDuration window length as text, e.g. {@code "00:01:00"}, may be {@code null}
 * @param codeVersion    computing code version, may be {@code null}
 */
public record FeatureDefinitionSummary(
        String name,
        String version,
        String description,
        String outputKind,
        String windowDuration,
        String codeVersion
) {
}
