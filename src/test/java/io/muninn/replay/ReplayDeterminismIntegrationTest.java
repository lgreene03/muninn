package io.muninn.replay;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.core.KafkaTemplate;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The proof: a live run and a subsequent replay of the same input produce
 * byte-identical computational outputs (modulo {@code eventId} — see ADR-0002).
 *
 * <p>This is the load-bearing test for Muninn's central architectural claim
 * (see {@code DETERMINISTIC_REPLAY.md}). If this test ever fails, determinism
 * has been broken.</p>
 *
 * <p>Pipeline under test:</p>
 * <pre>
 *   produce 6 trades  →  events.trade
 *                         ↓ (live engine)
 *                       features.vwap.1m.v1            ← captured as LIVE
 *
 *   submit ReplayJob(events.trade, [from, to))
 *                         ↓ (replay engine: same code, same windowing)
 *                       features.vwap.1m.v1.replay     ← captured as REPLAY
 *
 *   assert: LIVE outputs == REPLAY outputs on
 *           (windowStart, windowEnd, value, inputEventIds.size())
 * </pre>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
@Tag("integration")
class ReplayDeterminismIntegrationTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine")
    ).withDatabaseName("muninn_test").withUsername("muninn").withPassword("muninn");

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("muninn.ingestion.binance.enabled", () -> "false");
        registry.add("muninn.features.engine.enabled", () -> "true");
        registry.add("muninn.features.engine.window-duration", () -> "PT1M");
        registry.add("muninn.features.engine.checkpoint-interval", () -> "PT5M");
        registry.add("muninn.features.engine.code-version", () -> "test");
        registry.add("muninn.features.engine.late-event-policy", () -> "drop");
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private TestRestTemplate restTemplate;

    @LocalServerPort
    private int port;

    @Test
    void liveAndReplayProduceIdenticalOutputs() throws Exception {
        // --- Arrange: a stable reference time in the recent past ---
        Instant ref = Instant.now().minus(Duration.ofMinutes(10)).truncatedTo(ChronoUnit.MINUTES);
        Instant from = ref;
        Instant to = ref.plus(Duration.ofMinutes(3));

        Instrument btc = new Instrument(
                "BTC-USDT", "BTC", "USDT",
                new Exchange("binance", "Binance Spot", ZoneId.of("UTC"))
        );

        List<TradeEvent> trades = List.of(
                trade(btc, ref.plusSeconds(5),   1, "60000.00", "0.10", Side.BUY,  "t1"),
                trade(btc, ref.plusSeconds(30),  2, "60010.00", "0.05", Side.BUY,  "t2"),
                trade(btc, ref.plusSeconds(55),  3, "60005.00", "0.20", Side.SELL, "t3"),
                trade(btc, ref.plusSeconds(65),  4, "60020.00", "0.10", Side.BUY,  "t4"),
                trade(btc, ref.plusSeconds(90),  5, "60030.00", "0.15", Side.BUY,  "t5"),
                // The last event's watermark advance closes window [ref+1m, ref+2m)
                trade(btc, ref.plusSeconds(125), 6, "60025.00", "0.10", Side.SELL, "t6")
        );

        // --- Act: send all trades into events.trade ---
        for (TradeEvent t : trades) {
            kafkaTemplate.send("events.trade", "BTC-USDT", t).get(5, java.util.concurrent.TimeUnit.SECONDS);
        }

        // --- Wait: live engine produces VWAP outputs for the two closed windows ---
        List<FeatureComputedEvent> liveOutputs = consume(
                "features.vwap.1m.v1",
                "live-collector-" + UUID.randomUUID(),
                2,
                Duration.ofSeconds(45));

        assertThat(liveOutputs).as("live VWAP outputs").hasSize(2);

        // --- Act: submit a replay job for the same range ---
        String body = """
                {
                  "topics": ["events.trade"],
                  "from": "%s",
                  "to": "%s",
                  "featureVersion": "v1"
                }
                """.formatted(from, to);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<Map> submitResp = restTemplate.postForEntity(
                "/api/v1/replay/jobs",
                new HttpEntity<>(body, headers),
                Map.class);

        assertThat(submitResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        // --- Wait: replay produces VWAP outputs on the .replay sibling topic ---
        List<FeatureComputedEvent> replayOutputs = consume(
                "features.vwap.1m.v1.replay",
                "replay-collector-" + UUID.randomUUID(),
                2,
                Duration.ofSeconds(60));

        assertThat(replayOutputs).as("replay VWAP outputs").hasSize(2);

        // --- Assert: byte-identical on the determinism-critical fields ---
        liveOutputs.sort(Comparator.comparing(FeatureComputedEvent::windowStart));
        replayOutputs.sort(Comparator.comparing(FeatureComputedEvent::windowStart));

        for (int i = 0; i < liveOutputs.size(); i++) {
            FeatureComputedEvent live = liveOutputs.get(i);
            FeatureComputedEvent replay = replayOutputs.get(i);

            assertThat(replay.windowStart()).as("windowStart at index %d", i).isEqualTo(live.windowStart());
            assertThat(replay.windowEnd()).as("windowEnd at index %d", i).isEqualTo(live.windowEnd());
            assertThat(replay.featureName()).as("featureName at index %d", i).isEqualTo(live.featureName());
            assertThat(replay.featureVersion()).as("featureVersion at index %d", i).isEqualTo(live.featureVersion());
            assertThat(replay.codeVersion()).as("codeVersion at index %d", i).isEqualTo(live.codeVersion());
            assertThat(replay.value())
                    .as("value at index %d (BigDecimal equality)", i)
                    .usingComparator(BigDecimal::compareTo)
                    .isEqualTo(live.value());
            assertThat(replay.inputEventIds())
                    .as("inputEventIds count at index %d", i)
                    .hasSameSizeAs(live.inputEventIds());
            // NOTE: eventId is intentionally NOT compared. See docs/adr/0002-event-id-determinism.md.
        }
    }

    private TradeEvent trade(Instrument inst, Instant eventTime, long seq, String price, String size, Side side, String tradeId) {
        return new TradeEvent(
                UUIDv7.generate(),
                eventTime,
                Instant.now(),
                "integration-test",
                inst,
                seq,
                1,
                new BigDecimal(price),
                new BigDecimal(size),
                side,
                tradeId
        );
    }

    @SuppressWarnings("unchecked")
    private List<FeatureComputedEvent> consume(String topic, String groupId, int expected, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "io.muninn.shared.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, FeatureComputedEvent.class.getName());
        // Tell the JSON deserializer not to use type headers (we know the type).
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        List<FeatureComputedEvent> collected = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        try (KafkaConsumer<String, FeatureComputedEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (collected.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, FeatureComputedEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, FeatureComputedEvent> r : records) {
                    collected.add(r.value());
                    if (collected.size() >= expected) break;
                }
            }
        }
        return collected;
    }
}
