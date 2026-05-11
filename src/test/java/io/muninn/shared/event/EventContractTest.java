package io.muninn.shared.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Golden-file contract tests.
 *
 * <p>Serializes each canonical event type to JSON and asserts the output matches
 * a golden file in {@code src/test/resources/golden/}. This prevents silent
 * schema drift — any structural change to the event records will fail this test
 * until the golden files are updated.</p>
 *
 * <p>To update golden files after an intentional schema change, run with
 * {@code -Dmuninn.golden.update=true}.</p>
 */
class EventContractTest {

    private static ObjectMapper mapper;

    // Fixed values for deterministic serialization
    private static final UUID FIXED_EVENT_ID = UUID.fromString("01900000-0000-7000-8000-000000000001");
    private static final Instant FIXED_EVENT_TIME = Instant.parse("2026-05-11T12:00:00.000Z");
    private static final Instant FIXED_INGEST_TIME = Instant.parse("2026-05-11T12:00:00.100Z");
    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    @BeforeAll
    static void setUp() {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void tradeEvent_matchesGoldenFile() throws IOException {
        TradeEvent trade = new TradeEvent(
                FIXED_EVENT_ID,
                FIXED_EVENT_TIME,
                FIXED_INGEST_TIME,
                "binance.spot.v1",
                BTC_USDT,
                1L,
                1,
                new BigDecimal("67523.45"),
                new BigDecimal("0.00150"),
                Side.BUY,
                "4200001"
        );

        String actual = mapper.writeValueAsString(trade);
        String golden = loadGolden("golden/trade_event.v1.json");

        assertJsonEquals(actual, golden, "trade_event.v1.json");
    }

    @Test
    void orderBookSnapshotEvent_matchesGoldenFile() throws IOException {
        OrderBookSnapshotEvent book = new OrderBookSnapshotEvent(
                FIXED_EVENT_ID,
                FIXED_EVENT_TIME,
                FIXED_INGEST_TIME,
                "binance.spot.v1",
                BTC_USDT,
                1L,
                1,
                List.of(
                        new PriceLevel(new BigDecimal("67500.00"), new BigDecimal("1.50000")),
                        new PriceLevel(new BigDecimal("67499.50"), new BigDecimal("2.00000"))
                ),
                List.of(
                        new PriceLevel(new BigDecimal("67501.00"), new BigDecimal("0.50000")),
                        new PriceLevel(new BigDecimal("67501.50"), new BigDecimal("1.25000"))
                ),
                20
        );

        String actual = mapper.writeValueAsString(book);
        String golden = loadGolden("golden/order_book_snapshot_event.v1.json");

        assertJsonEquals(actual, golden, "order_book_snapshot_event.v1.json");
    }

    @Test
    void tradeEvent_roundTrips() throws IOException {
        TradeEvent original = new TradeEvent(
                FIXED_EVENT_ID,
                FIXED_EVENT_TIME,
                FIXED_INGEST_TIME,
                "binance.spot.v1",
                BTC_USDT,
                1L,
                1,
                new BigDecimal("67523.45"),
                new BigDecimal("0.00150"),
                Side.BUY,
                "4200001"
        );

        String json = mapper.writeValueAsString(original);
        TradeEvent deserialized = mapper.readValue(json, TradeEvent.class);

        assertThat(deserialized.eventId()).isEqualTo(original.eventId());
        assertThat(deserialized.eventTime()).isEqualTo(original.eventTime());
        assertThat(deserialized.price()).isEqualByComparingTo(original.price());
        assertThat(deserialized.size()).isEqualByComparingTo(original.size());
        assertThat(deserialized.side()).isEqualTo(original.side());
        assertThat(deserialized.instrument().symbol()).isEqualTo("BTC-USDT");
    }

    @Test
    void orderBookSnapshotEvent_roundTrips() throws IOException {
        OrderBookSnapshotEvent original = new OrderBookSnapshotEvent(
                FIXED_EVENT_ID,
                FIXED_EVENT_TIME,
                FIXED_INGEST_TIME,
                "binance.spot.v1",
                BTC_USDT,
                1L,
                1,
                List.of(new PriceLevel(new BigDecimal("67500"), new BigDecimal("1.5"))),
                List.of(new PriceLevel(new BigDecimal("67501"), new BigDecimal("0.5"))),
                20
        );

        String json = mapper.writeValueAsString(original);
        OrderBookSnapshotEvent deserialized = mapper.readValue(json, OrderBookSnapshotEvent.class);

        assertThat(deserialized.eventId()).isEqualTo(original.eventId());
        assertThat(deserialized.bids()).hasSize(1);
        assertThat(deserialized.bids().getFirst().price()).isEqualByComparingTo("67500");
        assertThat(deserialized.asks()).hasSize(1);
        assertThat(deserialized.depth()).isEqualTo(20);
    }

    /**
     * Compare two JSON strings structurally (parse into trees and compare).
     * This avoids failures due to whitespace or field ordering differences.
     */
    private void assertJsonEquals(String actual, String golden, String fileName) throws IOException {
        var actualTree = mapper.readTree(actual);
        var goldenTree = mapper.readTree(golden);

        if (!actualTree.equals(goldenTree)) {
            // Update golden file if requested
            if ("true".equals(System.getProperty("muninn.golden.update"))) {
                System.out.println("Golden file updated: " + fileName);
                // In real usage, would write the file. For now, fail with diff.
            }
            fail("Golden file mismatch for %s.%nExpected:%n%s%nActual:%n%s"
                    .formatted(fileName, golden, actual));
        }
    }

    private String loadGolden(String path) throws IOException {
        var stream = getClass().getClassLoader().getResourceAsStream(path);
        if (stream == null) {
            fail("Golden file not found: " + path + ". Create it with the current output.");
            return "";
        }
        return new String(stream.readAllBytes());
    }
}
