package io.muninn.query;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.query.backend.FeatureQueryBackend;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FeatureQueryService} at the abstraction level.
 *
 * <p>Backend-specific SQL shape lives in the per-backend tests
 * ({@code DuckDbFeatureQueryBackendTest}, {@code TrinoFeatureQueryBackendTest}).
 * This class verifies that the service delegates correctly, tags metrics with
 * the active backend id, and propagates results unchanged.</p>
 */
@ExtendWith(MockitoExtension.class)
class FeatureQueryServiceTest {

    @Mock
    private FeatureQueryBackend backend;

    private SimpleMeterRegistry registry;
    private FeatureQueryService service;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        when(backend.backendId()).thenReturn("duckdb");
        service = new FeatureQueryService(backend, registry);
    }

    @Test
    void queryFeature_delegatesToBackendWithExactArguments() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = Instant.parse("2026-05-01T01:00:00Z");
        when(backend.queryFeatureTimeSeries(any(), any(), any(), any()))
                .thenReturn(List.of(Map.of("window_start", from.toString(), "vwap_value", "60000.00")));

        List<Map<String, Object>> result = service.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(result).hasSize(1);
        verify(backend).queryFeatureTimeSeries(eq("vwap"), eq("BTC-USDT"), eq(from), eq(to));
    }

    @Test
    void queryFeature_returnsBackendResultUnchanged() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = Instant.parse("2026-05-01T01:00:00Z");
        List<Map<String, Object>> expected = List.of(
                Map.of("window_start", from.toString(), "value", 1.0),
                Map.of("window_start", to.toString(), "value", 2.0)
        );
        when(backend.queryFeatureTimeSeries(any(), any(), any(), any())).thenReturn(expected);

        List<Map<String, Object>> result = service.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void metrics_tagWithBackendId() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = Instant.parse("2026-05-01T01:00:00Z");
        when(backend.queryFeatureTimeSeries(any(), any(), any(), any())).thenReturn(List.of());

        service.queryFeature("vwap", "BTC-USDT", from, to);
        service.queryFeature("vwap", "BTC-USDT", from, to);

        double count = registry.find("muninn.query.requests").tag("backend", "duckdb").counter().count();
        assertThat(count).isEqualTo(2.0);

        long latencyCount = registry.find("muninn.query.latency").tag("backend", "duckdb").timer().count();
        assertThat(latencyCount).isEqualTo(2L);
    }
}
