package io.muninn.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DuckDbQueryService {

    private static final Logger log = LoggerFactory.getLogger(DuckDbQueryService.class);

    private final DataSource duckDbDataSource;

    public DuckDbQueryService(DataSource duckDbDataSource) {
        this.duckDbDataSource = duckDbDataSource;
    }

    public List<Map<String, Object>> query(String sql, Object... params) {
        log.debug("Executing DuckDB query: {}", sql);
        try (Connection conn = duckDbDataSource.getConnection();
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
            throw new RuntimeException("DuckDB query failed: " + sql, e);
        }
    }
}
