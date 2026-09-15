package io.muninn.feature.microstructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.muninn.feature.engine.WatermarkTracker;
import io.muninn.feature.engine.WindowManager;
import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit and golden-dataset tests for {@link OrderBookImbalanceComputer}.
 * Pure-function tests — no Spring context, no IO other than reading the fixture.
 */
class OrderBookImbalanceComputerTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instant WINDOW_START = Instant.parse("2026-05-11T14:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-05-11T14:01:00Z");
    private static final String CODE_VERSION = "test-sha-001";
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void compute_buyPressure_positiveObi() {
        OrderBookSnapshotEvent snapshot = snapshot(
                List.of(level("100", "10")), List.of(level("101", "2")), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        Optional<FeatureComputedEvent> result = OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION);

        assertThat(result).isPresent();
        // obi = (10-2)/12 = 0.66666667
        assertThat(result.get().value()).isEqualByComparingTo("0.66666667");
        assertThat(result.get().featureName()).isEqualTo("obi");
        assertThat(result.get().featureVersion()).isEqualTo("v1");
        assertThat(result.get().inputEventIds()).containsExactly(snapshot.eventId());
    }

    @Test
    void compute_sellPressure_negativeObi() {
        OrderBookSnapshotEvent snapshot = snapshot(
                List.of(level("100", "1")), List.of(level("101", "9")), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        Optional<FeatureComputedEvent> result = OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION);

        assertThat(result).isPresent();
        // obi = (1-9)/10 = -0.80000000
        assertThat(result.get().value()).isEqualByComparingTo("-0.80000000");
    }

    @Test
    void compute_onlyLastSnapshotInWindow_isUsed() {
        OrderBookSnapshotEvent stale = snapshot(
                List.of(level("100", "100")), List.of(level("101", "1")), "2026-05-11T14:00:05Z");
        OrderBookSnapshotEvent latest = snapshot(
                List.of(level("100", "1")), List.of(level("101", "1")), "2026-05-11T14:00:50Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(stale, latest));

        Optional<FeatureComputedEvent> result = OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION);

        assertThat(result).isPresent();
        assertThat(result.get().value()).isEqualByComparingTo("0.00000000"); // balanced 1 vs 1
        assertThat(result.get().inputEventIds()).containsExactly(latest.eventId());
    }

    @Test
    void compute_windowWithNoSnapshot_returnsEmpty() {
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of());

        assertThat(OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION)).isEmpty();
    }

    @Test
    void compute_nonPositiveLevels_throws() {
        OrderBookSnapshotEvent snapshot = snapshot(
                List.of(level("100", "1")), List.of(level("101", "1")), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        assertThatThrownBy(() -> OrderBookImbalanceComputer.compute(batch, 0, CODE_VERSION))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void compute_isDeterministic_sameInputsSameOutput() {
        OrderBookSnapshotEvent snapshot = snapshot(
                List.of(level("100", "4")), List.of(level("101", "6")), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent r1 = OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION).orElseThrow();
        FeatureComputedEvent r2 = OrderBookImbalanceComputer.compute(batch, 5, CODE_VERSION).orElseThrow();

        assertThat(r1.value()).isEqualByComparingTo(r2.value());
        assertThat(r1.values()).isEqualTo(r2.values());
        assertThat(r1.inputEventIds()).isEqualTo(r2.inputEventIds());
    }

    @Test
    void goldenDataset_producesExpectedObiValues() throws IOException {
        JsonNode inputSnapshots = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/obi/input_snapshots.json"));
        JsonNode expectedOutputs = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/obi/expected_obi_output.json"));

        List<OrderBookSnapshotEvent> snapshots = new ArrayList<>();
        for (JsonNode node : inputSnapshots) {
            snapshots.add(snapshotFromJson(node));
        }

        WatermarkTracker watermarkTracker = new WatermarkTracker();
        WindowManager windowManager = new WindowManager(Duration.ofMinutes(1), watermarkTracker);
        for (OrderBookSnapshotEvent snapshot : snapshots) {
            windowManager.add(snapshot, 0);
        }
        watermarkTracker.advance(0, Instant.parse("2026-05-11T14:05:00Z"));

        List<WindowedBatch> fired = new ArrayList<>();
        windowManager.fireCompletedWindows(fired::add);

        assertThat(fired).hasSize(expectedOutputs.size());

        for (int i = 0; i < expectedOutputs.size(); i++) {
            JsonNode expected = expectedOutputs.get(i);
            int windowIndex = i;
            FeatureComputedEvent actual = OrderBookImbalanceComputer.compute(fired.get(i), 2, CODE_VERSION)
                    .orElseThrow(() -> new AssertionError("expected an OBI result for window " + windowIndex));

            assertThat(actual.windowStart()).isEqualTo(Instant.parse(expected.get("windowStart").asText()));
            assertThat(actual.windowEnd()).isEqualTo(Instant.parse(expected.get("windowEnd").asText()));
            assertThat(actual.value())
                    .as("OBI for window %d", i)
                    .isEqualByComparingTo(expected.get("expectedObi").asText());
            assertThat(actual.values().get("levels"))
                    .as("levels used for window %d", i)
                    .isEqualByComparingTo(BigDecimal.valueOf(expected.get("expectedLevels").asInt()));
        }
    }

    // --- Helpers ---

    private OrderBookSnapshotEvent snapshotFromJson(JsonNode node) {
        List<PriceLevel> bids = new ArrayList<>();
        for (JsonNode b : node.get("bids")) {
            bids.add(level(b.get("price").asText(), b.get("size").asText()));
        }
        List<PriceLevel> asks = new ArrayList<>();
        for (JsonNode a : node.get("asks")) {
            asks.add(level(a.get("price").asText(), a.get("size").asText()));
        }
        Instant eventTime = Instant.parse(node.get("eventTime").asText());
        return new OrderBookSnapshotEvent(
                UUIDv7.generate(), eventTime, eventTime, "golden-test", BTC_USDT,
                1L, 1, bids, asks, Math.max(bids.size(), asks.size()));
    }

    private PriceLevel level(String price, String size) {
        return new PriceLevel(new BigDecimal(price), new BigDecimal(size));
    }

    private OrderBookSnapshotEvent snapshot(List<PriceLevel> bids, List<PriceLevel> asks, String eventTime) {
        Instant t = Instant.parse(eventTime);
        return new OrderBookSnapshotEvent(
                UUIDv7.generate(), t, t, "test", BTC_USDT, 1L, 1, bids, asks, Math.max(bids.size(), asks.size()));
    }
}
