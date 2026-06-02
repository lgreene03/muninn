package io.muninn.query;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the Phase 5 Query API surface.
 *
 * <p>Verifies HTTP contracts: status codes, response body shape, OpenAPI endpoint availability.
 * Does NOT assert on {@code points} content — the Parquet warehouse is empty in CI.</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@Testcontainers
@Tag("integration")
class QueryApiIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine")
    ).withDatabaseName("muninn_test").withUsername("muninn").withPassword("muninn");

    @Container
    @ServiceConnection
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    static {
        kafka.start();
        postgres.start();
        System.setProperty("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("muninn.ingestion.binance.enabled", () -> "false");
        registry.add("muninn.features.engine.enabled", () -> "false");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @LocalServerPort
    private int port;

    // --- Happy path ---

    @Test
    void queryFeature_validRange_returns200WithExpectedShape() {
        String url = "/api/v1/features/vwap"
                + "?instrument=BTC-USDT"
                + "&from=2026-05-01T00:00:00Z"
                + "&to=2026-05-01T01:00:00Z";

        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsKeys("feature", "version", "instrument", "from", "to", "points");
        assertThat(response.getBody().get("feature")).isEqualTo("vwap.1m");
        assertThat(response.getBody().get("version")).isEqualTo("v1");
        assertThat(response.getBody().get("instrument")).isEqualTo("BTC-USDT");
    }

    // --- Validation ---

    @Test
    void queryFeature_fromAfterTo_returns400() {
        String url = "/api/v1/features/vwap"
                + "?instrument=BTC-USDT"
                + "&from=2026-05-01T02:00:00Z"
                + "&to=2026-05-01T01:00:00Z";

        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void queryFeature_missingToParam_returns400() {
        String url = "/api/v1/features/vwap"
                + "?instrument=BTC-USDT"
                + "&from=2026-05-01T00:00:00Z";

        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // --- OpenAPI ---

    @Test
    void apiDocs_returns200WithOpenApiSpec() {
        ResponseEntity<Map> response = restTemplate.getForEntity("/api-docs", Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).containsKey("openapi");
    }

    @Test
    void swaggerUi_returns2xxOrRedirect() {
        ResponseEntity<String> response = restTemplate.getForEntity("/swagger-ui.html", String.class);

        // SpringDoc redirects /swagger-ui.html → /swagger-ui/index.html; both are valid
        assertThat(response.getStatusCode().is2xxSuccessful()
                || response.getStatusCode().is3xxRedirection()).isTrue();
    }
}
