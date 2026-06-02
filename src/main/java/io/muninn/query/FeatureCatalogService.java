package io.muninn.query;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Serves the catalog of registered feature definitions for the discovery
 * endpoint {@code GET /api/v1/features}.
 *
 * <p>Reads the Flyway-managed {@code feature_definitions} table (migration
 * {@code V004}) from the primary PostgreSQL datastore. This is distinct from
 * {@link FeatureQueryService}, which reads computed feature <em>time-series</em>
 * from the warehouse via a {@code FeatureQueryBackend}; the catalog is
 * operational metadata, not feature output.</p>
 */
@Service
public class FeatureCatalogService {

    private final JdbcTemplate jdbcTemplate;

    public FeatureCatalogService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * @return all active feature definitions, ordered by name. The
     *         {@code description} and {@code codeVersion} fields are not
     *         persisted in {@code feature_definitions} and are returned as
     *         {@code null}.
     */
    public List<FeatureDefinitionSummary> listDefinitions() {
        return jdbcTemplate.query(
                """
                SELECT name,
                       version,
                       aggregation,
                       window_duration::text AS window_duration
                FROM feature_definitions
                WHERE active = TRUE
                ORDER BY name
                """,
                (rs, rowNum) -> new FeatureDefinitionSummary(
                        rs.getString("name"),
                        rs.getString("version"),
                        null,
                        rs.getString("aggregation"),
                        rs.getString("window_duration"),
                        null
                ));
    }
}
