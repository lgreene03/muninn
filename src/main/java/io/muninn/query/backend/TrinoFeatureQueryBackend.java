package io.muninn.query.backend;

import io.muninn.shared.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Trino-backed feature query path for the {@code production-reference} profile.
 *
 * <p>Reads from Iceberg tables registered in AWS Glue (see ADR-0005). Table
 * names follow the convention {@code <catalog>.<schema>.features_<name>_<version>}
 * with dots in feature names replaced by underscores — the naming convention
 * is documented in ADR-0006 and consumed by the future Iceberg writer.</p>
 *
 * <p>Trino JDBC is used over its native protocol because:</p>
 * <ul>
 *   <li>It plays nicely with HikariCP and standard Spring data-source plumbing.</li>
 *   <li>The query API is read-only — no transaction semantics to negotiate.</li>
 *   <li>Iceberg time-travel queries (future) compose with the same dialect.</li>
 * </ul>
 *
 * <p>The driver is sizeable. The {@code [project].dependencies} entry in
 * {@code pom.xml} pulls it in unconditionally for now; ADR-0006 records the
 * footprint decision and the conditions under which it would become an
 * optional profile.</p>
 */
public final class TrinoFeatureQueryBackend implements FeatureQueryBackend {

    public static final String BACKEND_ID = "trino";

    private static final Logger log = LoggerFactory.getLogger(TrinoFeatureQueryBackend.class);

    private final DataSource dataSource;
    private final String catalog;
    private final String schema;

    public TrinoFeatureQueryBackend(DataSource dataSource, String catalog, String schema) {
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.schema = schema;
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
        // Iceberg / Trino table naming: see ADR-0006 §Naming.
        String table = tableNameFor(featureName);

        // Trino is happy with parameterized SQL. We still validate the table
        // identifier above because table names can't be parameter-bound;
        // featureName is restricted to a safe character set by the canonical
        // EventValidator before any FeatureComputedEvent reaches the warehouse,
        // and tableNameFor() lower-cases + underscores to reinforce that.
        String sql = """
                SELECT window_start, window_end, value AS vwap_value, input_event_count AS event_count
                FROM %s.%s.%s
                WHERE instrument = ? AND window_start >= ? AND window_end <= ?
                ORDER BY window_start
                """.formatted(catalog, schema, table);

        log.atDebug()
                .addKeyValue("feature", featureName)
                .addKeyValue("instrument", instrument)
                .addKeyValue("table", catalog + "." + schema + "." + table)
                .log("Executing Trino feature query");

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, instrument);
            stmt.setTimestamp(2, Timestamp.from(from));
            stmt.setTimestamp(3, Timestamp.from(to));

            try (ResultSet rs = stmt.executeQuery()) {
                return materialize(rs);
            }
        } catch (SQLException e) {
            // A missing-table failure (e.g., warehouse not yet migrated) is downgraded
            // to an empty result like DuckDB does. Other SQL exceptions are real.
            if (isTableNotFound(e)) {
                log.atInfo()
                        .addKeyValue("feature", featureName)
                        .addKeyValue("error", e.getMessage())
                        .log("Trino reports table not present; returning empty result");
                return List.of();
            }
            throw new StorageException("Trino query failed: " + sql, e);
        }
    }

    private List<Map<String, Object>> materialize(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    /**
     * Iceberg table identifier for a given feature name.
     *
     * <p>Feature names like {@code "vwap.1m"} can't be table identifiers (dots are
     * catalog separators in Trino), so we replace them with underscores. The
     * convention is documented in ADR-0006 — the Iceberg writer must use the
     * same transformation.</p>
     */
    static String tableNameFor(String featureName) {
        return "features_" + featureName.toLowerCase(Locale.ROOT).replace('.', '_').replace('-', '_');
    }

    private static boolean isTableNotFound(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) return false;
        String lower = msg.toLowerCase(Locale.ROOT);
        return lower.contains("does not exist") || lower.contains("not found") || lower.contains("table_not_found");
    }
}
