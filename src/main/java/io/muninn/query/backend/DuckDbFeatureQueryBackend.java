package io.muninn.query.backend;

import io.muninn.query.FeatureQueryInputValidator;
import io.muninn.storage.QueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * DuckDB-backed feature query path.
 *
 * <p>Reads Parquet partitions directly via DuckDB's {@code read_parquet()}
 * with Hive partitioning. Used by every {@code local-*} profile and the
 * default for {@code cloud-cheap}.</p>
 *
 * <p>The SQL dialect is DuckDB-specific (the {@code read_parquet} table
 * function and Hive partition globbing). The Trino backend uses a different
 * dialect against Iceberg-registered tables; that's why this lives behind
 * the {@link FeatureQueryBackend} abstraction rather than as raw SQL in
 * {@code FeatureQueryService}.</p>
 *
 * <p>Empty result for "no Parquet yet" is treated as an empty list, not an
 * error — see commentary in the catch block.</p>
 */
public final class DuckDbFeatureQueryBackend implements FeatureQueryBackend {

    public static final String BACKEND_ID = "duckdb";

    private static final Logger log = LoggerFactory.getLogger(DuckDbFeatureQueryBackend.class);

    private final QueryService queryService;
    private final String warehouseUri;

    /**
     * @param queryService low-level SQL executor against DuckDB.
     * @param warehouseUri base URI of the Parquet warehouse — {@code s3://muninn-warehouse}
     *                     in cloud profiles, a local path in {@code local-*}.
     */
    public DuckDbFeatureQueryBackend(QueryService queryService, String warehouseUri) {
        this.queryService = queryService;
        this.warehouseUri = trimTrailingSlash(warehouseUri);
    }

    @Override
    public String backendId() {
        return BACKEND_ID;
    }

    @Override
    public List<Map<String, Object>> queryFeatureTimeSeries(
            String featureName,
            String instrument,
            Instant from,
            Instant to
    ) {
        // DuckDB's read_parquet() is parameter-unfriendly for table-function args,
        // so we string-interpolate the path and time bounds. featureName and
        // instrument are therefore re-validated here against strict allowlists
        // (defense in depth; the controller boundary is the primary check) so a
        // quote/space/glob/path-traversal character can never reach the SQL string.
        FeatureQueryInputValidator.requireValidFeatureName(featureName);
        FeatureQueryInputValidator.requireValidInstrument(instrument);

        String path = "%s/features.%s.v1/instrument=%s/**/*.parquet"
                .formatted(warehouseUri, featureName, instrument);

        String sql = """
                SELECT window_start, window_end, vwap_value, event_count
                FROM read_parquet('%s', hive_partitioning=true)
                WHERE window_start >= '%s' AND window_end <= '%s'
                ORDER BY window_start
                """.formatted(path, from, to);

        try {
            return queryService.query(sql);
        } catch (Exception e) {
            // The Parquet warehouse may not be populated yet — common in early dev,
            // and not an error condition from the user's perspective. Log at debug
            // and return empty so the API still answers cleanly.
            log.atDebug()
                    .addKeyValue("feature", featureName)
                    .addKeyValue("instrument", instrument)
                    .addKeyValue("error", e.getMessage())
                    .log("DuckDB query returned no data");
            return List.of();
        }
    }

    private static String trimTrailingSlash(String uri) {
        if (uri == null || uri.isBlank()) return "s3://muninn-warehouse";
        return uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
    }
}
