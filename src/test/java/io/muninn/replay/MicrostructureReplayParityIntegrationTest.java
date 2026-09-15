package io.muninn.replay;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
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
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
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
import org.testcontainers.kafka.ConfluentKafkaContainer;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extends {@link ReplayDeterminismIntegrationTest}'s proof to the three computers
 * wired up in the P2 roadmap (OBI, micro-price, VPIN), not just VWAP.
 *
 * <p>Deliberately narrow: one window, one book snapshot, a handful of trades — enough
 * to exercise every registered computer's live-vs-replay path once, on the SAME shared
 * {@code WindowManager<MarketEvent>} the P2 rewrite introduced. Golden-value correctness
 * for each computer already has dedicated coverage (see
 * {@code OrderBookImbalanceComputerTest}, {@code MicroPriceComputerTest},
 * {@code VPINComputerTest}); this test's job is proving the wiring, not the maths.</p>
 *
 * <p>NOTE: could not be executed in the environment this was authored in — no Docker
 * daemon was available (Testcontainers requires one). It mirrors
 * {@code ReplayDeterminismIntegrationTest}'s established, working pattern closely; see
 * the PR description for what remains to be verified in CI.</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@Testcontainers
@ActiveProfiles("test")
@Tag("integration")
class MicrostructureReplayParityIntegrationTest {

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
        registry.add("muninn.features.engine.enabled", () -> "true");
        registry.add("muninn.features.engine.window-duration", () -> "PT1M");
        registry.add("muninn.features.engine.checkpoint-interval", () -> "PT5M");
        registry.add("muninn.features.engine.code-version", () -> "test");
        registry.add("muninn.features.engine.late-event-policy", () -> "drop");
        registry.add("muninn.features.engine.obi-levels", () -> "5");
        // Sized so the window's trades exactly fill one VPIN bucket — see the fixture below.
        registry.add("muninn.features.engine.vpin-bucket-volume-size", () -> "0.35");
        registry.add("muninn.features.engine.vpin-number-of-buckets", () -> "1");
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private TestRestTemplate restTemplate;

    @LocalServerPort
    private int port;

    @Test
    void liveAndReplayProduceIdenticalOutputs_forAllFourRegisteredFeatures() throws Exception {
        Instant ref = Instant.now().minus(Duration.ofMinutes(10)).truncatedTo(ChronoUnit.MINUTES);
        Instant from = ref;
        Instant to = ref.plus(Duration.ofMinutes(2));

        Instrument btc = new Instrument(
                "BTC-USDT", "BTC", "USDT",
                new Exchange("binance", "Binance Spot", ZoneId.of("UTC"))
        );

        List<TradeEvent> trades = List.of(
                trade(btc, ref.plusSeconds(5),  1, "60000.00", "0.10", Side.BUY,  "t1"),
                trade(btc, ref.plusSeconds(30), 2, "60010.00", "0.05", Side.BUY,  "t2"),
                trade(btc, ref.plusSeconds(55), 3, "60005.00", "0.20", Side.SELL, "t3"),
                // Past the window boundary — advances the watermark and closes window 1.
                trade(btc, ref.plusSeconds(65), 4, "60020.00", "0.10", Side.BUY,  "t4")
        );
        OrderBookSnapshotEvent snapshot = new OrderBookSnapshotEvent(
                UUIDv7.generate(), ref.plusSeconds(40), ref.plusSeconds(40), "integration-test", btc, 1L, 1,
                List.of(new PriceLevel(new BigDecimal("59995.00"), new BigDecimal("9.00"))),
                List.of(new PriceLevel(new BigDecimal("60005.00"), new BigDecimal("1.00"))),
                1
        );

        for (TradeEvent t : trades) {
            kafkaTemplate.send("events.trade", "BTC-USDT", t).get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
        kafkaTemplate.send("events.book.snapshot", "BTC-USDT", snapshot).get(5, java.util.concurrent.TimeUnit.SECONDS);

        List<FeatureComputedEvent> liveVwap = consume("features.vwap.1m.v1", "live-vwap-" + UUID.randomUUID(), 1, Duration.ofSeconds(45));
        List<FeatureComputedEvent> liveObi = consume("features.obi.v1", "live-obi-" + UUID.randomUUID(), 1, Duration.ofSeconds(45));
        List<FeatureComputedEvent> liveMicroPrice = consume("features.micro_price.v1", "live-mp-" + UUID.randomUUID(), 1, Duration.ofSeconds(45));
        List<FeatureComputedEvent> liveVpin = consume("features.vpin.v1", "live-vpin-" + UUID.randomUUID(), 1, Duration.ofSeconds(45));

        assertThat(liveVwap).as("live VWAP").hasSize(1);
        assertThat(liveObi).as("live OBI").hasSize(1);
        assertThat(liveMicroPrice).as("live micro_price").hasSize(1);
        assertThat(liveVpin).as("live VPIN").hasSize(1);

        String body = """
                {
                  "topics": ["events.trade", "events.book.snapshot"],
                  "from": "%s",
                  "to": "%s",
                  "featureVersion": "v1"
                }
                """.formatted(from, to);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<Map> submitResp = restTemplate.postForEntity(
                "/api/v1/replay/jobs", new HttpEntity<>(body, headers), Map.class);
        assertThat(submitResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        List<FeatureComputedEvent> replayVwap = consume("features.vwap.1m.v1.replay", "replay-vwap-" + UUID.randomUUID(), 1, Duration.ofSeconds(60));
        List<FeatureComputedEvent> replayObi = consume("features.obi.v1.replay", "replay-obi-" + UUID.randomUUID(), 1, Duration.ofSeconds(60));
        List<FeatureComputedEvent> replayMicroPrice = consume("features.micro_price.v1.replay", "replay-mp-" + UUID.randomUUID(), 1, Duration.ofSeconds(60));
        List<FeatureComputedEvent> replayVpin = consume("features.vpin.v1.replay", "replay-vpin-" + UUID.randomUUID(), 1, Duration.ofSeconds(60));

        assertIdentical(liveVwap.get(0), replayVwap, "vwap.1m");
        assertIdentical(liveObi.get(0), replayObi, "obi");
        assertIdentical(liveMicroPrice.get(0), replayMicroPrice, "micro_price");
        assertIdentical(liveVpin.get(0), replayVpin, "vpin");
    }

    private void assertIdentical(FeatureComputedEvent live, List<FeatureComputedEvent> replayList, String feature) {
        assertThat(replayList).as("replay %s", feature).hasSize(1);
        FeatureComputedEvent replay = replayList.get(0);

        assertThat(replay.windowStart()).as("%s windowStart", feature).isEqualTo(live.windowStart());
        assertThat(replay.windowEnd()).as("%s windowEnd", feature).isEqualTo(live.windowEnd());
        assertThat(replay.featureName()).as("%s featureName", feature).isEqualTo(live.featureName());
        assertThat(replay.featureVersion()).as("%s featureVersion", feature).isEqualTo(live.featureVersion());
        assertThat(replay.value())
                .as("%s value (BigDecimal equality)", feature)
                .usingComparator(BigDecimal::compareTo)
                .isEqualTo(live.value());
        assertThat(replay.inputEventIds())
                .as("%s inputEventIds count", feature)
                .hasSameSizeAs(live.inputEventIds());
    }

    private TradeEvent trade(Instrument inst, Instant eventTime, long seq, String price, String size, Side side, String tradeId) {
        return new TradeEvent(
                UUIDv7.generate(), eventTime, Instant.now(), "integration-test", inst,
                seq, 1, new BigDecimal(price), new BigDecimal(size), side, tradeId);
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
