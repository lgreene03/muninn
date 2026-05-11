package io.muninn.ingestion.adapter;

import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link BinanceBookParser}.
 * Uses recorded Binance depth20 payloads from src/test/resources/fixtures/binance/.
 */
class BinanceBookParserTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final String SOURCE = "binance.spot.v1";

    @Test
    void parse_validDepthMessage_producesCorrectSnapshot() throws IOException {
        String json = loadFixture("fixtures/binance/depth20.json");
        Instant ingestTime = Instant.parse("2026-05-11T12:00:00.500Z");

        OrderBookSnapshotEvent book = BinanceBookParser.parse(json, BTC_USDT, SOURCE, ingestTime, 7L);

        assertThat(book.eventId()).isNotNull();
        assertThat(book.eventTime()).isEqualTo(ingestTime);
        assertThat(book.ingestTime()).isEqualTo(ingestTime);
        assertThat(book.source()).isEqualTo(SOURCE);
        assertThat(book.instrument()).isEqualTo(BTC_USDT);
        assertThat(book.sequenceNumber()).isEqualTo(7L);
        assertThat(book.schemaVersion()).isEqualTo(1);

        assertThat(book.bids()).hasSize(3);
        assertThat(book.bids().getFirst().price()).isEqualByComparingTo("67500.00");
        assertThat(book.bids().getFirst().size()).isEqualByComparingTo("1.50000");

        assertThat(book.asks()).hasSize(3);
        assertThat(book.asks().getFirst().price()).isEqualByComparingTo("67501.00");
        assertThat(book.asks().getFirst().size()).isEqualByComparingTo("0.50000");
    }

    @Test
    void parse_zeroSizeLevels_areFiltered() {
        String json = """
                {"lastUpdateId":100,"bids":[["67500","1.5"],["67499","0"]],"asks":[["67501","0.5"]]}
                """;

        OrderBookSnapshotEvent book = BinanceBookParser.parse(json, BTC_USDT, SOURCE, Instant.now(), 1L);

        // Zero-size bid at 67499 should be filtered out
        assertThat(book.bids()).hasSize(1);
        assertThat(book.bids().getFirst().price()).isEqualByComparingTo("67500");
    }

    @Test
    void parse_invalidJson_throwsBinanceParseException() {
        assertThatThrownBy(() -> BinanceBookParser.parse(
                "not valid json", BTC_USDT, SOURCE, Instant.now(), 1L
        )).isInstanceOf(BinanceParseException.class)
                .hasMessageContaining("Failed to parse Binance book snapshot");
    }

    @Test
    void parse_depthMatchesLargestSide() {
        String json = """
                {"lastUpdateId":100,"bids":[["67500","1.5"],["67499","2.0"],["67498","0.5"]],"asks":[["67501","0.5"]]}
                """;

        OrderBookSnapshotEvent book = BinanceBookParser.parse(json, BTC_USDT, SOURCE, Instant.now(), 1L);
        assertThat(book.depth()).isEqualTo(3); // max(3 bids, 1 ask)
    }

    private String loadFixture(String path) throws IOException {
        return new String(getClass().getClassLoader().getResourceAsStream(path).readAllBytes());
    }
}
