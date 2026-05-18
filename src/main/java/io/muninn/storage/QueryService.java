package io.muninn.storage;

import java.util.List;
import java.util.Map;

/**
 * Contract for executing analytical SQL queries against the storage layer.
 * The primary implementation reads from DuckDB over Parquet files in MinIO.
 */
public interface QueryService {

    /**
     * Execute a parameterized SQL query and return results as a list of maps.
     *
     * @param sql    the SQL query with parameter placeholders
     * @param params the parameter values
     * @return a list of rows, each represented as an ordered map of column name → value
     */
    List<Map<String, Object>> query(String sql, Object... params);
}
