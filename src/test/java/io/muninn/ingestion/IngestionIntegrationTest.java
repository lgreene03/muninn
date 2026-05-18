package io.muninn.ingestion;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration test: synthetic event → HTTP endpoint → Redpanda → consume → assert.
 *
 * <p>Uses Testcontainers to spin up real Kafka and PostgreSQL instances.
 * This is the contract-level test that proves the ingestion pipeline works end-to-end.</p>
 *
 * <p>Tagged as "integration" — excluded from normal {@code mvn test} runs.
 * Run with: {@code mvn test -Dgroups=integration}</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
@Tag("integration")
class IngestionIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine")
    ).withDatabaseName("muninn_test")
            .withUsername("muninn")
            .withPassword("muninn");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    static {
        // See ReplayDeterminismIntegrationTest for the rationale.
        kafka.start();
        postgres.start();
        System.setProperty("spring.kafka.bootstrap-servers", kafka.getBootstrapServers());
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("muninn.ingestion.binance.enabled", () -> "false");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @LocalServerPort
    private int port;

    @Test
    void postTradeEvent_appearsInKafkaTopic() {
        // --- Arrange: build a synthetic trade event ---
        String eventId = UUIDv7.generate().toString();
        Instant now = Instant.now();

        String tradeJson = """
                {
                  "eventId": "%s",
                  "eventTime": "%s",
                  "ingestTime": "%s",
                  "source": "integration-test",
                  "instrument": {
                    "symbol": "BTC-USDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "exchange": {
                      "id": "binance",
                      "displayName": "Binance Spot",
                      "timezone": "UTC"
                    }
                  },
                  "sequenceNumber": 1,
                  "schemaVersion": 1,
                  "price": 67500.50,
                  "size": 0.01,
                  "side": "BUY",
                  "exchangeTradeId": "integration-test-001"
                }
                """.formatted(eventId, now, now);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // --- Act: POST to the ingestion endpoint ---
        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/api/v1/events/trade",
                new HttpEntity<>(tradeJson, headers),
                Map.class
        );

        // --- Assert: HTTP response ---
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).containsEntry("status", "accepted");
        assertThat(response.getBody()).containsKey("eventId");

        // --- Assert: event appears in Kafka topic ---
        try (KafkaConsumer<String, TradeEvent> consumer = createConsumer()) {
            consumer.subscribe(List.of("events.trade"));

            ConsumerRecords<String, TradeEvent> records = ConsumerRecords.empty();
            long deadline = System.currentTimeMillis() + 10_000;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records = consumer.poll(Duration.ofMillis(500));
            }

            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            var record = records.iterator().next();
            assertThat(record.topic()).isEqualTo("events.trade");
            assertThat(record.key()).isEqualTo("BTC-USDT");
            assertThat(record.value().eventId()).isNotNull();
            assertThat(record.value().source()).isEqualTo("integration-test");
        }
    }

    @Test
    void postInvalidTradeEvent_returnsBadRequest() {
        // Trade with a very old eventTime should fail validation
        String tradeJson = """
                {
                  "eventId": "%s",
                  "eventTime": "2019-01-01T00:00:00Z",
                  "ingestTime": "%s",
                  "source": "integration-test",
                  "instrument": {
                    "symbol": "BTC-USDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "exchange": {
                      "id": "binance",
                      "displayName": "Binance Spot",
                      "timezone": "UTC"
                    }
                  },
                  "sequenceNumber": 1,
                  "schemaVersion": 1,
                  "price": 67500.50,
                  "size": 0.01,
                  "side": "BUY",
                  "exchangeTradeId": "bad-event-001"
                }
                """.formatted(UUIDv7.generate(), Instant.now());

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/api/v1/events/trade",
                new HttpEntity<>(tradeJson, headers),
                Map.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).containsEntry("status", "rejected");
        assertThat(response.getBody()).containsKey("reasons");
    }

    @Test
    void healthEndpoint_returnsUp() {
        ResponseEntity<Map> response = restTemplate.getForEntity("/actuator/health", Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void prometheusEndpoint_isAccessible() {
        ResponseEntity<String> response = restTemplate.getForEntity("/actuator/prometheus", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("jvm_");
    }

    private KafkaConsumer<String, TradeEvent> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "io.muninn.shared.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, TradeEvent.class.getName());
        // Application publishes with type-info headers (via Spring's KafkaTemplate
        // and the configured type mapping). The test consumer accepts the typed
        // record directly.
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new KafkaConsumer<>(props);
    }
}
