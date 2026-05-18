package io.muninn.query.backend;

import io.muninn.storage.QueryService;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class DuckDbFeatureQueryBackendTest {

    @Test
    void backendId_isDuckdb() {
        var backend = new DuckDbFeatureQueryBackend((sql, params) -> List.of(), "s3://example");
        assertThat(backend.backendId()).isEqualTo("duckdb");
    }

    @Test
    void queryFeatureTimeSeries_buildsHivePartitionedSqlAgainstWarehouseUri() {
        AtomicReference<String> capturedSql = new AtomicReference<>();
        QueryService spy = new QueryService() {
            @Override
            public List<Map<String, Object>> query(String sql, Object... params) {
                capturedSql.set(sql);
                return List.of(Map.of("window_start", "2026-05-10T14:00:00Z", "vwap_value", 60000.0));
            }
        };

        var backend = new DuckDbFeatureQueryBackend(spy, "s3://muninn-warehouse");

        var rows = backend.queryFeatureTimeSeries(
                "vwap.1m", "BTC-USDT",
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z"));

        assertThat(rows).hasSize(1);
        assertThat(capturedSql.get())
                .contains("read_parquet")
                .contains("features.vwap.1m.v1/instrument=BTC-USDT")
                .contains("hive_partitioning=true")
                .contains("2026-05-10T14:00:00Z")
                .contains("2026-05-10T15:00:00Z")
                .contains("ORDER BY window_start");
    }

    @Test
    void queryFeatureTimeSeries_returnsEmptyListWhenWarehouseEmpty() {
        // The Parquet warehouse may not be populated yet — common in dev — and
        // that's not an error condition from the user's perspective.
        QueryService throwing = (sql, params) -> { throw new RuntimeException("no such file"); };
        var backend = new DuckDbFeatureQueryBackend(throwing, "s3://muninn-warehouse");

        var rows = backend.queryFeatureTimeSeries(
                "vwap.1m", "BTC-USDT",
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z"));

        assertThat(rows).isEmpty();
    }

    @Test
    void constructor_trimsTrailingSlashOnWarehouseUri() {
        AtomicReference<String> capturedSql = new AtomicReference<>();
        QueryService spy = (sql, params) -> { capturedSql.set(sql); return List.of(); };

        var backend = new DuckDbFeatureQueryBackend(spy, "s3://muninn-warehouse/");
        backend.queryFeatureTimeSeries(
                "vwap.1m", "BTC-USDT",
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z"));

        // No double slash between warehouse root and feature path.
        assertThat(capturedSql.get()).doesNotContain("s3://muninn-warehouse//features");
        assertThat(capturedSql.get()).contains("s3://muninn-warehouse/features");
    }
}
