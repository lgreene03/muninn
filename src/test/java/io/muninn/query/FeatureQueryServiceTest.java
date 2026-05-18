package io.muninn.query;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.storage.QueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FeatureQueryService}.
 * Verifies delegation, SQL shape, metric emission, and error resilience.
 */
@ExtendWith(MockitoExtension.class)
class FeatureQueryServiceTest {

    @Mock
    private QueryService duckDbQueryService;

    private SimpleMeterRegistry registry;
    private FeatureQueryService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        service = new FeatureQueryService(duckDbQueryService, registry);
    }

    // --- Delegation ---

    @Test
    void queryFeature_delegatesToDuckDb_withNonNullResult() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to   = Instant.parse("2026-05-01T01:00:00Z");

        when(duckDbQueryService.query(anyString()))
                .thenReturn(List.of(Map.of("window_start", from.toString(), "vwap_value", "60000.00")));

        List<Map<String, Object>> result = service.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(result).isNotNull().hasSize(1);
        verify(duckDbQueryService).query(anyString());
    }

    // --- SQL Shape regression (A3) ---

    @Test
    void queryFeature_sqlContainsRequiredClauses() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to   = Instant.parse("2026-05-01T01:00:00Z");
        when(duckDbQueryService.query(anyString())).thenReturn(List.of());

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        service.queryFeature("vwap", "BTC-USDT", from, to);

        verify(duckDbQueryService).query(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("read_parquet(");
        assertThat(sql).contains("vwap");
        assertThat(sql).containsIgnoringCase("order by");
    }

    // --- Error resilience ---

    @Test
    void queryFeature_whenDuckDbThrows_returnsEmptyList() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to   = Instant.parse("2026-05-01T01:00:00Z");

        when(duckDbQueryService.query(anyString()))
                .thenThrow(new RuntimeException("DuckDB unavailable"));

        List<Map<String, Object>> result = service.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(result).isNotNull().isEmpty();
    }

    // --- Metrics ---

    @Test
    void queryFeature_incrementsRequestCounter() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to   = Instant.parse("2026-05-01T01:00:00Z");
        when(duckDbQueryService.query(anyString())).thenReturn(List.of());

        service.queryFeature("vwap", "BTC-USDT", from, to);
        service.queryFeature("vwap", "BTC-USDT", from, to);

        double count = registry.find("muninn.query.requests").counter().count();
        assertThat(count).isEqualTo(2.0);
    }

    @Test
    void queryFeature_recordsLatencyTimer() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to   = Instant.parse("2026-05-01T01:00:00Z");
        when(duckDbQueryService.query(anyString())).thenReturn(List.of());

        service.queryFeature("vwap", "BTC-USDT", from, to);

        long count = registry.find("muninn.query.latency").timer().count();
        assertThat(count).isEqualTo(1L);
    }
}
