package io.muninn.query.backend;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The storage-engine abstraction the Query API delegates feature reads to.
 *
 * <p>Two implementations exist:</p>
 * <ul>
 *   <li>{@link DuckDbFeatureQueryBackend} — reads Parquet directly via DuckDB's
 *       {@code read_parquet()} + Hive partitioning. The default for every
 *       {@code local-*} profile.</li>
 *   <li>{@link TrinoFeatureQueryBackend} — issues SQL through the Trino JDBC
 *       driver against an Iceberg catalog (Glue in the {@code production-reference}
 *       profile). Selected by setting {@code muninn.query.backend=trino}.</li>
 * </ul>
 *
 * <p>The Query API layer ({@code FeatureQueryController}, {@code FeatureQueryService})
 * depends only on this interface — never on a concrete backend. An ArchUnit rule
 * enforces that. Adding a third backend is a matter of providing one more
 * implementation and wiring it in {@link FeatureQueryBackendConfiguration}.</p>
 *
 * <p>Rationale and table-naming conventions are documented in
 * {@code docs/adr/0006-trino-query-backend.md}.</p>
 */
public interface FeatureQueryBackend {

    /**
     * Fetch a feature's time-series for a single instrument within an event-time range.
     *
     * @param featureName feature identifier (e.g., {@code "vwap.1m"})
     * @param instrument  instrument symbol (e.g., {@code "BTC-USDT"})
     * @param from        inclusive start of the event-time range
     * @param to          exclusive end of the event-time range
     * @return rows as ordered maps of column name to value; empty list when the
     *         warehouse has no matching data (not an error)
     */
    List<Map<String, Object>> queryFeatureTimeSeries(
            String featureName,
            String instrument,
            Instant from,
            Instant to
    );

    /**
     * @return a stable identifier for the backend (e.g., {@code "duckdb"}, {@code "trino"}).
     *         Surfaced in the {@code muninn.query.requests} metric so live dashboards
     *         can distinguish which engine answered.
     */
    String backendId();
}
