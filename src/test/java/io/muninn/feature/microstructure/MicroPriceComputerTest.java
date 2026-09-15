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
 * Unit and golden-dataset tests for {@link MicroPriceComputer}.
 * Pure-function tests — no Spring context, no IO other than reading the fixture.
 */
class MicroPriceComputerTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instant WINDOW_START = Instant.parse("2026-05-11T14:00:00Z");
    private static final Instant WINDOW_END = Instant.parse("2026-05-11T14:01:00Z");
    private static final String CODE_VERSION = "test-sha-001";
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void compute_asymmetricSizes_weightsTowardThinnerSide() {
        // Bid is much larger than ask -> price pulled toward the ask (the thinner side).
        OrderBookSnapshotEvent snapshot = snapshot(
                level("100", "9"), level("101", "1"), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent result = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();

        // microPrice = (100*1 + 101*9) / 10 = (100 + 909) / 10 = 100.9
        assertThat(result.value()).isEqualByComparingTo("100.90000000");
        assertThat(result.featureName()).isEqualTo("micro_price");
    }

    @Test
    void compute_liesStrictlyBetweenBestBidAndBestAsk() {
        OrderBookSnapshotEvent snapshot = snapshot(
                level("100", "7"), level("102", "3"), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent result = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();

        assertThat(result.value())
                .as("microPrice must lie strictly between best bid and best ask")
                .isGreaterThan(new BigDecimal("100"))
                .isLessThan(new BigDecimal("102"));
    }

    @Test
    void compute_symmetricSizes_equalsSimpleMidpoint() {
        OrderBookSnapshotEvent snapshot = snapshot(
                level("100", "5"), level("102", "5"), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent result = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();

        assertThat(result.value()).isEqualByComparingTo("101.00000000");
    }

    @Test
    void compute_zeroSizeBothSides_fallsBackToSimpleMidpoint() {
        OrderBookSnapshotEvent snapshot = snapshot(
                level("100", "0"), level("102", "0"), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent result = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();

        assertThat(result.value()).isEqualByComparingTo("101.00000000");
    }

    @Test
    void compute_emptyBidsOrAsks_returnsEmpty() {
        OrderBookSnapshotEvent snapshot = new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.parse("2026-05-11T14:00:30Z"), Instant.parse("2026-05-11T14:00:30Z"),
                "test", BTC_USDT, 1L, 1, List.of(), List.of(level("101", "1")), 1);
        // depth must be positive; asks non-empty satisfies constructor, bids empty is what we test
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        assertThat(MicroPriceComputer.compute(batch, CODE_VERSION)).isEmpty();
    }

    @Test
    void compute_windowWithNoSnapshot_returnsEmpty() {
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of());

        assertThat(MicroPriceComputer.compute(batch, CODE_VERSION)).isEmpty();
    }

    @Test
    void compute_isDeterministic_sameInputsSameOutput() {
        OrderBookSnapshotEvent snapshot = snapshot(
                level("100", "4"), level("102", "6"), "2026-05-11T14:00:30Z");
        WindowedBatch batch = new WindowedBatch(WINDOW_START, WINDOW_END, List.of(snapshot));

        FeatureComputedEvent r1 = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();
        FeatureComputedEvent r2 = MicroPriceComputer.compute(batch, CODE_VERSION).orElseThrow();

        assertThat(r1.value()).isEqualByComparingTo(r2.value());
        assertThat(r1.values()).isEqualTo(r2.values());
    }

    @Test
    void goldenDataset_producesExpectedMicroPriceValues() throws IOException {
        JsonNode inputSnapshots = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/micro_price/input_snapshots.json"));
        JsonNode expectedOutputs = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/micro_price/expected_micro_price_output.json"));

        List<OrderBookSnapshotEvent> snapshots = new ArrayList<>();
        for (JsonNode node : inputSnapshots) {
            Instant t = Instant.parse(node.get("eventTime").asText());
            JsonNode b = node.get("bids").get(0);
            JsonNode a = node.get("asks").get(0);
            snapshots.add(new OrderBookSnapshotEvent(
                    UUIDv7.generate(), t, t, "golden-test", BTC_USDT, snapshots.size() + 1L, 1,
                    List.of(level(b.get("price").asText(), b.get("size").asText())),
                    List.of(level(a.get("price").asText(), a.get("size").asText())),
                    1));
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
            FeatureComputedEvent actual = MicroPriceComputer.compute(fired.get(i), CODE_VERSION)
                    .orElseThrow(() -> new AssertionError("expected a micro_price result for window " + windowIndex));

            assertThat(actual.windowStart()).isEqualTo(Instant.parse(expected.get("windowStart").asText()));
            assertThat(actual.value())
                    .as("micro_price for window %d", i)
                    .isEqualByComparingTo(expected.get("expectedMicroPrice").asText());
        }
    }

    // --- Helpers ---

    private PriceLevel level(String price, String size) {
        return new PriceLevel(new BigDecimal(price), new BigDecimal(size));
    }

    private OrderBookSnapshotEvent snapshot(PriceLevel bid, PriceLevel ask, String eventTime) {
        Instant t = Instant.parse(eventTime);
        return new OrderBookSnapshotEvent(
                UUIDv7.generate(), t, t, "test", BTC_USDT, 1L, 1, List.of(bid), List.of(ask), 1);
    }
}
