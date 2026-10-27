package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Pinned against real messages captured from OKX's live public feed
 * (refresh with {@code scripts/probe_okx.py src/test/resources/okx}).
 */
class OkxMessageParserTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant INGEST = Instant.parse("2026-10-08T00:00:00Z");

    private final AtomicLong trades = new AtomicLong();
    private final AtomicLong books = new AtomicLong();
    private final OkxMessageParser parser = new OkxMessageParser(trades::incrementAndGet, books::incrementAndGet);

    private static List<String> fixture(String name) throws IOException {
        try (InputStream in = OkxMessageParserTest.class.getResourceAsStream("/okx/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().filter(l -> !l.isBlank()).toList();
        }
    }

    @Test
    void everyRealBooks5MessageBecomesOneUsableSnapshot() throws IOException {
        for (String raw : fixture("books5.jsonl")) {
            OkxMessageParser.Result r = parser.parse(raw, INGEST);
            assertThat(r.problem()).as(raw).isNull();
            assertThat(r.events()).hasSize(1);
            OrderBookSnapshotEvent book = (OrderBookSnapshotEvent) r.events().getFirst();

            assertThat(book.source()).isEqualTo("okx.spot.v1");
            assertThat(book.instrument().symbol()).isEqualTo("BTC-USDT");
            assertThat(book.instrument().exchange().id()).isEqualTo("okx");
            assertThat(book.bids()).hasSizeBetween(1, 5);
            assertThat(book.asks()).hasSizeBetween(1, 5);
            // Bids best-first descending, asks best-first ascending, and not crossed.
            assertThat(book.bids()).extracting(PriceLevel::price).isSortedAccordingTo((a, b) -> b.compareTo(a));
            assertThat(book.asks()).extracting(PriceLevel::price).isSorted();
            assertThat(book.bids().getFirst().price()).isLessThan(book.asks().getFirst().price());
        }
    }

    @Test
    void bookPricesAndSizesKeepTheVenuesExactDigits() throws IOException {
        String raw = fixture("books5.jsonl").getFirst();
        JsonNode level = MAPPER.readTree(raw).path("data").path(0).path("bids").path(0);
        OrderBookSnapshotEvent book = (OrderBookSnapshotEvent) parser.parse(raw, INGEST).events().getFirst();
        // Read from the string, so nothing is rounded through a double.
        assertThat(book.bids().getFirst().price().toPlainString()).isEqualTo(level.path(0).asText());
        assertThat(book.bids().getFirst().size().toPlainString()).isEqualTo(level.path(1).asText());
    }

    @Test
    void bookEventTimeIsTheExchangeTimestampNotArrival() throws IOException {
        String raw = fixture("books5.jsonl").getFirst();
        long ts = Long.parseLong(MAPPER.readTree(raw).path("data").path(0).path("ts").asText());
        OrderBookSnapshotEvent book = (OrderBookSnapshotEvent) parser.parse(raw, INGEST).events().getFirst();
        assertThat(book.eventTime()).isEqualTo(Instant.ofEpochMilli(ts));
        assertThat(book.ingestTime()).isEqualTo(INGEST);
    }

    @Test
    void everyRealTradeMapsTheTakerSideToTheAggressor() throws IOException {
        for (String raw : fixture("trades.jsonl")) {
            JsonNode t = MAPPER.readTree(raw).path("data").path(0);
            OkxMessageParser.Result r = parser.parse(raw, INGEST);
            assertThat(r.problem()).as(raw).isNull();
            TradeEvent trade = (TradeEvent) r.events().getFirst();

            // OKX reports the TAKER side, which IS the aggressor. (Coinbase reports the
            // maker side and would need inverting; OKX must not be inverted.)
            Side expected = "buy".equals(t.path("side").asText()) ? Side.BUY : Side.SELL;
            assertThat(trade.side()).isEqualTo(expected);
            assertThat(trade.price().toPlainString()).isEqualTo(t.path("px").asText());
            assertThat(trade.size().toPlainString()).isEqualTo(t.path("sz").asText());
            assertThat(trade.exchangeTradeId()).isEqualTo(t.path("tradeId").asText());
            // ts arrives as a JSON STRING of epoch milliseconds, not a number.
            assertThat(t.path("ts").isTextual()).isTrue();
            assertThat(trade.eventTime()).isEqualTo(Instant.ofEpochMilli(Long.parseLong(t.path("ts").asText())));
        }
    }

    @Test
    void sequenceNumbersAreIndependentPerStreamType() throws IOException {
        parser.parse(fixture("trades.jsonl").get(0), INGEST);
        parser.parse(fixture("trades.jsonl").get(1), INGEST);
        MarketEvent book = parser.parse(fixture("books5.jsonl").get(0), INGEST).events().getFirst();
        assertThat(book.sequenceNumber()).isEqualTo(1L);
        assertThat(trades.get()).isEqualTo(2L);
    }

    @Test
    void controlMessagesProduceNothingAndNoProblem() {
        assertThat(parser.parse("pong", INGEST)).isEqualTo(OkxMessageParser.Result.ok(List.of()));
        OkxMessageParser.Result ack = parser.parse(
                "{\"event\":\"subscribe\",\"arg\":{\"channel\":\"books5\",\"instId\":\"BTC-USDT\"},\"connId\":\"a\"}", INGEST);
        assertThat(ack.events()).isEmpty();
        assertThat(ack.problem()).isNull();
    }

    @Test
    void venueErrorsAreSurfacedNotSwallowed() {
        OkxMessageParser.Result r = parser.parse(
                "{\"event\":\"error\",\"code\":\"60012\",\"msg\":\"Invalid request\",\"connId\":\"a\"}", INGEST);
        assertThat(r.venueError()).isTrue();
        assertThat(r.problem()).contains("60012").contains("Invalid request");
    }

    @Test
    void malformedInputIsReportedNotThrown() {
        assertThat(parser.parse("{not json", INGEST).problem()).startsWith("not JSON");
        assertThat(parser.parse("{\"arg\":{\"channel\":\"trades\"}}", INGEST).problem()).contains("no data array");
    }

    @Test
    void aDerivativeIsRefusedRatherThanMislabelledAsSpot() {
        // The canonical instrument has no market-type field, so BTC-USDT-SWAP must never
        // be filed as the spot pair BTC-USDT.
        OkxMessageParser.Result r = parser.parse("{\"arg\":{\"channel\":\"trades\",\"instId\":\"BTC-USDT-SWAP\"},"
                + "\"data\":[{\"instId\":\"BTC-USDT-SWAP\",\"tradeId\":\"1\",\"px\":\"1\",\"sz\":\"1\","
                + "\"side\":\"buy\",\"ts\":\"1\"}]}", INGEST);
        assertThat(r.events()).isEmpty();
        assertThat(r.problem()).contains("not a spot instrument id");
    }

    @Test
    void zeroSizeLevelsAreDroppedAndAnEmptySideIsRefused() {
        String withZero = "{\"arg\":{\"channel\":\"books5\",\"instId\":\"BTC-USDT\"},\"data\":[{\"instId\":\"BTC-USDT\","
                + "\"ts\":\"1\",\"bids\":[[\"100\",\"1\",\"0\",\"1\"],[\"99\",\"0\",\"0\",\"0\"]],"
                + "\"asks\":[[\"101\",\"2\",\"0\",\"1\"]]}]}";
        OrderBookSnapshotEvent book = (OrderBookSnapshotEvent) parser.parse(withZero, INGEST).events().getFirst();
        assertThat(book.bids()).hasSize(1);

        String emptyAsks = withZero.replace("\"asks\":[[\"101\",\"2\",\"0\",\"1\"]]", "\"asks\":[]");
        OkxMessageParser.Result r = parser.parse(emptyAsks, INGEST);
        assertThat(r.events()).isEmpty();
        assertThat(r.problem()).contains("empty side");
    }
}
