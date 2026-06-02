package io.muninn.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FeatureQueryController}.
 * Verifies request validation, response shape, and delegation to the service layer.
 */
@ExtendWith(MockitoExtension.class)
class FeatureQueryControllerTest {

    @Mock
    private FeatureQueryService queryService;

    @InjectMocks
    private FeatureQueryController controller;

    @Test
    void queryFeature_validRange_returns200WithValuesEnvelope() {
        Instant start = Instant.parse("2026-05-01T00:00:00Z");
        Instant end = Instant.parse("2026-05-01T01:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", start, end))
                .thenReturn(List.of(Map.of("windowStart", start.toString(), "value", "60000.00")));

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", start, end, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertThat(body).containsOnlyKeys("values");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> values = (List<Map<String, Object>>) body.get("values");
        assertThat(values).hasSize(1);

        verify(queryService).queryFeature("vwap", "BTC-USDT", start, end);
    }

    @Test
    void queryFeature_startAfterEnd_returns400() {
        Instant start = Instant.parse("2026-05-01T02:00:00Z");
        Instant end = Instant.parse("2026-05-01T01:00:00Z");

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", start, end, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isInstanceOf(QueryErrorResponse.class);
        QueryErrorResponse error = (QueryErrorResponse) response.getBody();
        assertThat(error.status()).isEqualTo("error");
        assertThat(error.message()).contains("'start' must be before 'end'");

        verifyNoInteractions(queryService);
    }

    @Test
    void queryFeature_emptyResult_returns200WithEmptyValues() {
        Instant start = Instant.parse("2026-05-01T00:00:00Z");
        Instant end = Instant.parse("2026-05-01T01:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", start, end))
                .thenReturn(List.of());

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", start, end, null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> values = (List<Map<String, Object>>) body.get("values");
        assertThat(values).isEmpty();
    }

    @Test
    void queryFeature_equalStartAndEnd_returns200() {
        Instant same = Instant.parse("2026-05-01T00:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", same, same))
                .thenReturn(List.of());

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", same, same, null);

        // start == end is not "after", so it should be allowed (zero-width range)
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void queryFeature_limit_capsReturnedValues() {
        Instant start = Instant.parse("2026-05-01T00:00:00Z");
        Instant end = Instant.parse("2026-05-01T01:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", start, end))
                .thenReturn(List.of(
                        Map.of("value", "1"),
                        Map.of("value", "2"),
                        Map.of("value", "3")));

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", start, end, 2);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> values = (List<Map<String, Object>>) body.get("values");
        assertThat(values).hasSize(2);
    }
}
