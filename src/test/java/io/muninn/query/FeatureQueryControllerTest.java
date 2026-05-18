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
    void queryFeature_validRange_returns200WithExpectedShape() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = Instant.parse("2026-05-01T01:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", from, to))
                .thenReturn(List.of(Map.of("window_start", from.toString(), "vwap_value", "60000.00")));

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        assertThat(body).containsKey("feature");
        assertThat(body).containsKey("version");
        assertThat(body).containsKey("instrument");
        assertThat(body).containsKey("points");
        assertThat(body.get("feature")).isEqualTo("vwap.1m");
        assertThat(body.get("version")).isEqualTo("v1");
        assertThat(body.get("instrument")).isEqualTo("BTC-USDT");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> points = (List<Map<String, Object>>) body.get("points");
        assertThat(points).hasSize(1);

        verify(queryService).queryFeature("vwap", "BTC-USDT", from, to);
    }

    @Test
    void queryFeature_fromAfterTo_returns400() {
        Instant from = Instant.parse("2026-05-01T02:00:00Z");
        Instant to = Instant.parse("2026-05-01T01:00:00Z");

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isInstanceOf(QueryErrorResponse.class);
        QueryErrorResponse error = (QueryErrorResponse) response.getBody();
        assertThat(error.status()).isEqualTo("error");
        assertThat(error.message()).contains("'from' must be before 'to'");

        verifyNoInteractions(queryService);
    }

    @Test
    void queryFeature_emptyResult_returns200WithEmptyPoints() {
        Instant from = Instant.parse("2026-05-01T00:00:00Z");
        Instant to = Instant.parse("2026-05-01T01:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", from, to))
                .thenReturn(List.of());

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", from, to);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);

        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) response.getBody();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> points = (List<Map<String, Object>>) body.get("points");
        assertThat(points).isEmpty();
    }

    @Test
    void queryFeature_equalFromAndTo_returns200() {
        Instant same = Instant.parse("2026-05-01T00:00:00Z");

        when(queryService.queryFeature("vwap", "BTC-USDT", same, same))
                .thenReturn(List.of());

        ResponseEntity<Object> response = controller.queryFeature("vwap", "BTC-USDT", same, same);

        // from == to is not "after", so it should be allowed (zero-width range)
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}
