package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.TradeEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Binance's combined stream carries every configured instrument on one socket, so each
 * message must be attributed to the instrument its {@code stream} field names. Until
 * G0.18 the adapter subscribed to the first configured instrument only and labelled
 * every message with it: a second instrument would have been recorded as the first.
 */
class BinanceCombinedStreamTest {

    private static final Instant INGEST = Instant.parse("2026-11-10T12:00:00Z");
    private static final List<String> STREAMS = List.of("trade", "depth20@100ms");

    private final BinanceCombinedStream twoInstruments =
            new BinanceCombinedStream(List.of("btcusdt", "ethusdt"), STREAMS);

    private static String trade(String symbol, long tradeId, String price) {
        return "{\"stream\":\"" + symbol.toLowerCase() + "@trade\",\"data\":{\"e\":\"trade\",\"E\":1762776000000,"
                + "\"s\":\"" + symbol.toUpperCase() + "\",\"t\":" + tradeId + ",\"p\":\"" + price + "\","
                + "\"q\":\"0.5\",\"T\":1762776000000,\"m\":false,\"M\":true}}";
    }

    private static String book(String symbol, String bid, String ask) {
        return "{\"stream\":\"" + symbol.toLowerCase() + "@depth20@100ms\",\"data\":{\"lastUpdateId\":160,"
                + "\"bids\":[[\"" + bid + "\",\"1.0\"]],\"asks\":[[\"" + ask + "\",\"2.0\"]]}}";
    }

    @Test
    void subscribesEveryConfiguredInstrumentToEveryConfiguredStream() {
        assertThat(twoInstruments.path()).isEqualTo(
                "/stream?streams=btcusdt@trade/btcusdt@depth20@100ms/ethusdt@trade/ethusdt@depth20@100ms");
    }

    @Test
    void interleavedMessagesForTwoInstrumentsAreEachAttributedToTheirOwn() {
        List<String> wire = List.of(
                trade("BTCUSDT", 1, "67000.10"),
                trade("ETHUSDT", 2, "3500.20"),
                book("ETHUSDT", "3499.00", "3501.00"),
                book("BTCUSDT", "66999.00", "67001.00"),
                trade("ETHUSDT", 3, "3500.30"),
                trade("BTCUSDT", 4, "67000.40"));

        List<MarketEvent> events = new ArrayList<>();
        for (String message : wire) {
            BinanceCombinedStream.Result r = twoInstruments.route(message, INGEST);
            assertThat(r.problem()).as(message).isNull();
            events.add(r.event());
        }

        assertThat(events).extracting(e -> e.instrument().symbol())
                .containsExactly("BTC-USDT", "ETH-USDT", "ETH-USDT", "BTC-USDT", "ETH-USDT", "BTC-USDT");
        assertThat(events).allSatisfy(e -> {
            assertThat(e.source()).isEqualTo("binance.spot.v1");
            assertThat(e.instrument().exchange().id()).isEqualTo("binance");
        });
        // Each event carries its own instrument's prices, not the other's.
        assertThat(((TradeEvent) events.get(1)).price()).isEqualByComparingTo("3500.20");
        assertThat(((OrderBookSnapshotEvent) events.get(3)).bids().getFirst().price()).isEqualByComparingTo("66999.00");
        assertThat(events.get(1).instrument().baseAsset()).isEqualTo("ETH");
        assertThat(events.get(1).instrument().quoteAsset()).isEqualTo("USDT");
    }

    @Test
    void sequenceNumbersRunPerInstrumentAndPerEventTypeWithoutGaps() {
        List<String> wire = List.of(
                trade("BTCUSDT", 1, "1"), trade("ETHUSDT", 2, "1"), trade("BTCUSDT", 3, "1"),
                book("ETHUSDT", "1", "2"), book("BTCUSDT", "1", "2"), book("ETHUSDT", "1", "2"),
                trade("ETHUSDT", 4, "1"));

        List<String> seen = new ArrayList<>();
        for (String message : wire) {
            MarketEvent e = twoInstruments.route(message, INGEST).event();
            String kind = e instanceof TradeEvent ? "trade" : "book";
            seen.add(e.instrument().symbol() + " " + kind + " " + e.sequenceNumber());
        }

        assertThat(seen).containsExactly(
                "BTC-USDT trade 1", "ETH-USDT trade 1", "BTC-USDT trade 2",
                "ETH-USDT book 1", "BTC-USDT book 1", "ETH-USDT book 2",
                "ETH-USDT trade 2");
    }

    @Test
    void aStreamForAnInstrumentNotConfiguredIsIgnoredNotFiledUnderTheFirst() {
        BinanceCombinedStream.Result r = twoInstruments.route(trade("SOLUSDT", 9, "150.00"), INGEST);

        assertThat(r.event()).isNull();
        assertThat(r.problem()).contains("solusdt@trade");
    }

    @Test
    void aStreamNotSubscribedForAConfiguredInstrumentIsIgnored() {
        String aggTrade = "{\"stream\":\"btcusdt@aggTrade\",\"data\":{\"e\":\"aggTrade\",\"s\":\"BTCUSDT\","
                + "\"a\":1,\"p\":\"1\",\"q\":\"1\",\"T\":1762776000000,\"m\":false}}";

        BinanceCombinedStream.Result r = twoInstruments.route(aggTrade, INGEST);

        assertThat(r.event()).isNull();
        assertThat(r.problem()).contains("btcusdt@aggTrade");
    }

    @Test
    void aMessageWithNoStreamNameCannotBeAttributedAndIsIgnored() {
        String bare = "{\"e\":\"trade\",\"E\":1762776000000,\"s\":\"BTCUSDT\",\"t\":1,\"p\":\"1\",\"q\":\"1\","
                + "\"T\":1762776000000,\"m\":false,\"M\":true}";

        BinanceCombinedStream.Result r = twoInstruments.route(bare, INGEST);

        assertThat(r.event()).isNull();
        assertThat(r.problem()).isNotNull();
    }

    @Test
    void aTradeWhosePayloadSymbolDisagreesWithItsStreamIsRefused() {
        String mislabelled = trade("BTCUSDT", 1, "67000").replace("btcusdt@trade", "ethusdt@trade");

        BinanceCombinedStream.Result r = twoInstruments.route(mislabelled, INGEST);

        assertThat(r.event()).isNull();
        assertThat(r.problem()).contains("BTCUSDT").contains("ethusdt@trade");
    }

    @Test
    void unparseableInputIsReportedNeverThrown() {
        assertThat(twoInstruments.route("not json", INGEST).problem()).isNotNull();
        assertThat(twoInstruments.route(trade("BTCUSDT", 1, "not-a-price"), INGEST).problem()).isNotNull();
    }

    @Test
    void configuredSymbolsAreLowercasedAsBinanceStreamNamesRequire() {
        BinanceCombinedStream upper = new BinanceCombinedStream(List.of("BTCUSDT", "EthUsdt"), List.of("trade"));

        assertThat(upper.path()).isEqualTo("/stream?streams=btcusdt@trade/ethusdt@trade");
        assertThat(upper.route(trade("ETHUSDT", 1, "3500"), INGEST).event().instrument().symbol())
                .isEqualTo("ETH-USDT");
    }

    @Test
    void theFiveRecordedPairsAllRoute() {
        List<String> pairs = List.of("btcusdt", "ethusdt", "solusdt", "xrpusdt", "dogeusdt");
        BinanceCombinedStream five = new BinanceCombinedStream(pairs, STREAMS);

        assertThat(five.path().split("/", -1)).hasSize(1 + pairs.size() * STREAMS.size());
        assertThat(pairs).allSatisfy(p -> {
            assertThat(five.route(trade(p, 1, "1"), INGEST).event().instrument().symbol())
                    .isEqualTo(p.substring(0, p.length() - 4).toUpperCase() + "-USDT");
            assertThat(five.route(book(p, "1", "2"), INGEST).event().instrument().baseAsset())
                    .isEqualTo(p.substring(0, p.length() - 4).toUpperCase());
        });
    }

    @Test
    void moreStreamsThanOneConnectionCarriesAreRefusedAtStartUp() {
        List<String> many = new ArrayList<>();
        for (int i = 0; i < 513; i++) {
            many.add("coin" + i + "usdt");
        }

        assertThatThrownBy(() -> new BinanceCombinedStream(many, STREAMS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1026")
                .hasMessageContaining("1024");
        // Exactly at the limit is allowed.
        new BinanceCombinedStream(many.subList(0, 512), STREAMS);
    }

    @Test
    void aStreamTypeThatCannotBeParsedIsRefusedAtStartUp() {
        assertThatThrownBy(() -> new BinanceCombinedStream(List.of("btcusdt"), List.of("trade", "aggTrade")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("aggTrade");
        // The diff-depth stream carries deltas, not snapshots, and the book parser would
        // read it as an empty book.
        assertThatThrownBy(() -> new BinanceCombinedStream(List.of("btcusdt"), List.of("depth@100ms")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void twoDepthStreamsForOnePairAreRefusedAtStartUp() {
        // Both would be recorded as that pair's books under one sequence: two depths, or
        // the same depth twice, interleaved in one series that research cannot separate.
        assertThatThrownBy(() -> new BinanceCombinedStream(List.of("btcusdt"),
                List.of("trade", "depth5", "depth20@100ms")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("depth5")
                .hasMessageContaining("depth20@100ms");
        assertThatThrownBy(() -> new BinanceCombinedStream(List.of("btcusdt"), List.of("depth20", "depth20@100ms")))
                .isInstanceOf(IllegalArgumentException.class);
        // The same stream listed twice is one subscription, not two.
        new BinanceCombinedStream(List.of("btcusdt"), List.of("trade", "depth20@100ms", "depth20@100ms"));
    }

    @Test
    void noInstrumentsIsRefusedAtStartUp() {
        assertThatThrownBy(() -> new BinanceCombinedStream(List.of(), STREAMS))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
