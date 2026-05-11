package io.muninn.storage;

import io.muninn.config.DuckDbConnectionManager;
import io.muninn.shared.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Query service for DuckDB — the embedded analytical engine over Parquet files.
 *
 * <p>DuckDB is used as a read-only query layer; it has no persistent state.
 * Queries are executed against Parquet files in MinIO or on local disk.</p>
 */
@Service
public class DuckDbQueryService {

    private static final Logger log = LoggerFactory.getLogger(DuckDbQueryService.class);

    private final DuckDbConnectionManager connectionManager;

    public DuckDbQueryService(DuckDbConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    /**
     * Execute a parameterized SQL query and return results as a list of maps.
     *
     * @param sql    the SQL query with parameter placeholders
     * @param params the parameter values
     * @return a list of rows, each represented as an ordered map of column name → value
     * @throws StorageException if the query fails
     */
    public List<Map<String, Object>> query(String sql, Object... params) {
        log.atDebug()
                .addKeyValue("sql", sql)
                .log("Executing DuckDB query");

        try (Connection conn = connectionManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }

            ResultSet rs = stmt.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                results.add(row);
            }
            return results;
        } catch (SQLException e) {
            throw new StorageException("DuckDB query failed: " + sql, e);
        }
    }
}
