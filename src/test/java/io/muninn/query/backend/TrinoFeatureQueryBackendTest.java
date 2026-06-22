package io.muninn.query.backend;

import io.muninn.shared.exception.StorageException;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Behavior tests for the Trino backend. The full integration path needs
 * Testcontainers Trino + Iceberg + Glue and is not run here; the JDBC
 * surface is exercised against a mocked driver so the SQL shape and error
 * mapping are still pinned. Tracked: replace with a Testcontainers-based
 * integration test once a stable image set is in CI.
 */
class TrinoFeatureQueryBackendTest {

    @Test
    void backendId_isTrino() {
        var backend = new TrinoFeatureQueryBackend(mock(DataSource.class), "iceberg", "muninn");
        assertThat(backend.backendId()).isEqualTo("trino");
    }

    @Test
    void tableNameFor_lowercasesAndUnderscoresDotsAndDashes() {
        assertThat(TrinoFeatureQueryBackend.tableNameFor("vwap.1m")).isEqualTo("features_vwap_1m");
        assertThat(TrinoFeatureQueryBackend.tableNameFor("VPIN")).isEqualTo("features_vpin");
        assertThat(TrinoFeatureQueryBackend.tableNameFor("ob-imbalance.fast"))
                .isEqualTo("features_ob_imbalance_fast");
    }

    @Test
    void tableNameFor_rejectsInjectionInsteadOfSanitizing() {
        // A featureName with a quote/space/paren must be rejected outright; the
        // old char-replace would have let these through into the table identifier.
        assertThatThrownBy(() -> TrinoFeatureQueryBackend.tableNameFor("vwap\" OR \"1\"=\"1"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TrinoFeatureQueryBackend.tableNameFor("vwap; DROP TABLE x"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TrinoFeatureQueryBackend.tableNameFor("../etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void queryFeatureTimeSeries_issuesParameterizedSqlAgainstQualifiedTable() throws SQLException {
        AtomicReference<String> capturedSql = new AtomicReference<>();
        DataSource ds = stubDataSourceReturning(
                List.of(Map.of("window_start", "2026-05-10T14:00:00Z", "vwap_value", 60000.0)),
                capturedSql);

        var backend = new TrinoFeatureQueryBackend(ds, "iceberg", "muninn");

        var rows = backend.queryFeatureTimeSeries(
                "vwap.1m", "BTC-USDT",
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z"));

        assertThat(rows).hasSize(1);
        assertThat(capturedSql.get())
                .contains("iceberg.muninn.features_vwap_1m")
                .contains("WHERE instrument = ?")
                .contains("ORDER BY window_start");
    }

    @Test
    void queryFeatureTimeSeries_returnsEmptyOnTableNotFound() throws SQLException {
        DataSource ds = stubDataSourceThrowing(new SQLException("line 1:14: Table 'iceberg.muninn.features_vwap_1m' does not exist"));

        var backend = new TrinoFeatureQueryBackend(ds, "iceberg", "muninn");

        var rows = backend.queryFeatureTimeSeries(
                "vwap.1m", "BTC-USDT",
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z"));

        assertThat(rows).isEmpty();
    }

    @Test
    void queryFeatureTimeSeries_wrapsRealSqlErrorsAsStorageException() throws SQLException {
        DataSource ds = stubDataSourceThrowing(new SQLException("connection refused"));

        var backend = new TrinoFeatureQueryBackend(ds, "iceberg", "muninn");

        assertThatThrownBy(() -> backend.queryFeatureTimeSeries(
                "vwap.1m", "BTC-USDT",
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z")))
                .isInstanceOf(StorageException.class)
                .hasMessageContaining("Trino query failed");
    }

    // ----- helpers ----------------------------------------------------------

    private static DataSource stubDataSourceReturning(
            List<Map<String, Object>> rows,
            AtomicReference<String> capturedSql
    ) throws SQLException {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        PreparedStatement stmt = mock(PreparedStatement.class);
        ResultSet rs = stubResultSet(rows);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.prepareStatement(anyString())).thenAnswer(invocation -> {
            capturedSql.set(invocation.getArgument(0));
            return stmt;
        });
        when(stmt.executeQuery()).thenReturn(rs);
        return ds;
    }

    private static DataSource stubDataSourceThrowing(SQLException e) throws SQLException {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        PreparedStatement stmt = mock(PreparedStatement.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.prepareStatement(anyString())).thenReturn(stmt);
        when(stmt.executeQuery()).thenThrow(e);
        return ds;
    }

    private static ResultSet stubResultSet(List<Map<String, Object>> rows) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData meta = mock(ResultSetMetaData.class);

        // Build a flat columns view from the first row (deterministic order via LinkedHashMap-like).
        List<String> columns = new ArrayList<>(rows.isEmpty() ? List.of() : rows.get(0).keySet());

        when(rs.getMetaData()).thenReturn(meta);
        when(meta.getColumnCount()).thenReturn(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            when(meta.getColumnLabel(i + 1)).thenReturn(columns.get(i));
        }

        // next() yields true for each row then false.
        Iterator<Map<String, Object>> it = rows.iterator();
        AtomicReference<Map<String, Object>> current = new AtomicReference<>();
        when(rs.next()).thenAnswer(invocation -> {
            if (it.hasNext()) {
                current.set(it.next());
                return true;
            }
            return false;
        });
        when(rs.getObject(anyInt())).thenAnswer(invocation -> {
            int colIndex = invocation.getArgument(0);
            return current.get().get(columns.get(colIndex - 1));
        });
        return rs;
    }

    @SuppressWarnings("unused")
    private static Timestamp anyTs() {
        return Timestamp.from(Instant.EPOCH);
    }
}
