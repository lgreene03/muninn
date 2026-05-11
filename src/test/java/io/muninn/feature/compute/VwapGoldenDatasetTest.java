package io.muninn.feature.compute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.muninn.feature.engine.WatermarkTracker;
import io.muninn.feature.engine.WindowManager;
import io.muninn.feature.engine.WindowedBatch;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
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

import static org.assertj.core.api.Assertions.*;

/**
 * Golden dataset test for VWAP.
 *
 * <p>Reads synthetic trades from {@code datasets/vwap/input_trades.json},
 * feeds them through the {@link WindowManager} and {@link VwapComputer},
 * and asserts the output matches the hand-calculated values in
 * {@code datasets/vwap/expected_vwap_output.json}.</p>
 *
 * <p>This is the correctness anchor for the feature engine. Any change to
 * the computation that produces different output must explain why.</p>
 */
class VwapGoldenDatasetTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final String CODE_VERSION = "golden-test-v1";
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void goldenDataset_producesExpectedVwapValues() throws IOException {
        // --- Load golden dataset ---
        JsonNode inputTrades = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/vwap/input_trades.json"));
        JsonNode expectedOutputs = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/vwap/expected_vwap_output.json"));

        // --- Build trade events from the dataset ---
        List<TradeEvent> trades = new ArrayList<>();
        for (JsonNode node : inputTrades) {
            trades.add(new TradeEvent(
                    UUIDv7.generate(),
                    Instant.parse(node.get("eventTime").asText()),
                    Instant.parse(node.get("eventTime").asText()),
                    "golden-test",
                    BTC_USDT,
                    trades.size() + 1L,
                    1,
                    new BigDecimal(node.get("price").asText()),
                    new BigDecimal(node.get("size").asText()),
                    Side.BUY,
                    node.get("id").asText()
            ));
        }

        // --- Feed trades through WindowManager ---
        WatermarkTracker watermarkTracker = new WatermarkTracker();
        WindowManager windowManager = new WindowManager(Duration.ofMinutes(1), watermarkTracker);

        for (TradeEvent trade : trades) {
            windowManager.add(trade, 0); // single partition
        }

        // Force remaining windows to fire by advancing watermark past all of them
        // The last trade is at 14:04:10, so advance to 14:05:00 to close the 14:04 window
        watermarkTracker.advance(0, Instant.parse("2026-05-11T14:05:00Z"));

        List<WindowedBatch> firedWindows = new ArrayList<>();
        windowManager.fireCompletedWindows(firedWindows::add);

        // --- Compute VWAP for each fired window ---
        List<FeatureComputedEvent> results = new ArrayList<>();
        for (WindowedBatch batch : firedWindows) {
            results.add(VwapComputer.compute(batch, CODE_VERSION));
        }

        // --- Assert against golden expected output ---
        // Note: we expect 4 windows (14:00, 14:01, 14:02, 14:03)
        // The 14:04 window has 1 trade but also fires due to watermark advance
        assertThat(results.size()).isGreaterThanOrEqualTo(expectedOutputs.size());

        for (int i = 0; i < expectedOutputs.size(); i++) {
            JsonNode expected = expectedOutputs.get(i);
            FeatureComputedEvent actual = results.get(i);

            String windowLabel = expected.get("windowStart").asText() + " → " + expected.get("windowEnd").asText();

            assertThat(actual.windowStart())
                    .as("Window start for %s", windowLabel)
                    .isEqualTo(Instant.parse(expected.get("windowStart").asText()));

            assertThat(actual.windowEnd())
                    .as("Window end for %s", windowLabel)
                    .isEqualTo(Instant.parse(expected.get("windowEnd").asText()));

            BigDecimal expectedVwap = new BigDecimal(expected.get("expectedVwap").asText());
            assertThat(actual.value())
                    .as("VWAP for %s", windowLabel)
                    .isEqualByComparingTo(expectedVwap);

            int expectedTradeCount = expected.get("tradeCount").asInt();
            assertThat(actual.inputEventIds())
                    .as("Trade count for %s", windowLabel)
                    .hasSize(expectedTradeCount);
        }
    }

    @Test
    void goldenDataset_deterministic_twoRunsIdentical() throws IOException {
        // Run the exact same dataset twice and assert identical output
        JsonNode inputTrades = mapper.readTree(
                getClass().getClassLoader().getResourceAsStream("datasets/vwap/input_trades.json"));

        List<BigDecimal> run1 = runVwapPipeline(inputTrades);
        List<BigDecimal> run2 = runVwapPipeline(inputTrades);

        assertThat(run1).hasSize(run2.size());
        for (int i = 0; i < run1.size(); i++) {
            assertThat(run1.get(i))
                    .as("VWAP value at index %d", i)
                    .isEqualByComparingTo(run2.get(i));
        }
    }

    private List<BigDecimal> runVwapPipeline(JsonNode inputTrades) {
        List<TradeEvent> trades = new ArrayList<>();
        for (JsonNode node : inputTrades) {
            trades.add(new TradeEvent(
                    UUIDv7.generate(),
                    Instant.parse(node.get("eventTime").asText()),
                    Instant.parse(node.get("eventTime").asText()),
                    "golden-test",
                    BTC_USDT,
                    trades.size() + 1L, 1,
                    new BigDecimal(node.get("price").asText()),
                    new BigDecimal(node.get("size").asText()),
                    Side.BUY,
                    node.get("id").asText()
            ));
        }

        WatermarkTracker tracker = new WatermarkTracker();
        WindowManager wm = new WindowManager(Duration.ofMinutes(1), tracker);
        for (TradeEvent trade : trades) {
            wm.add(trade, 0);
        }
        tracker.advance(0, Instant.parse("2026-05-11T14:05:00Z"));

        List<BigDecimal> results = new ArrayList<>();
        wm.fireCompletedWindows(batch -> results.add(VwapComputer.compute(batch, CODE_VERSION).value()));
        return results;
    }
}
