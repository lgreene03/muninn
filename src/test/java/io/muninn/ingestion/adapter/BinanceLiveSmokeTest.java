package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.TradeEvent;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Runs the real adapter against Binance's live public combined stream for the five
 * recorded pairs. Off by default, so CI never depends on an external network or a
 * venue's uptime:
 *
 * <pre>mvn test -Dtest=BinanceLiveSmokeTest -Dmuninn.live=true</pre>
 *
 * <p>Public data only. Book payloads carry no symbol, so the fixture tests can only show
 * that attribution follows the stream name. This shows it on real traffic: every pair
 * delivers books, and each pair's books stay in that pair's own price band. A message
 * filed under the wrong pair would put, say, an ETH price among BTC's.
 */
@EnabledIfSystemProperty(named = "muninn.live", matches = "true")
class BinanceLiveSmokeTest {

    private static final List<String> PAIRS = List.of("btcusdt", "ethusdt", "solusdt", "xrpusdt", "dogeusdt");
    private static final List<String> SYMBOLS = List.of("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT");

    @Test
    void everyPairStreamsAndEachKeepsToItsOwnPriceBand() throws Exception {
        List<MarketEvent> events = new CopyOnWriteArrayList<>();
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        BinanceWebSocketAdapter adapter = new BinanceWebSocketAdapter(
                new BinanceConfig(true, null, PAIRS, null, null), meters);
        adapter.start(events::add);
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline && !tenBooksForEveryPair(events)) {
                Thread.sleep(200);
            }
        } finally {
            adapter.stop();
        }

        Map<String, List<OrderBookSnapshotEvent>> books = events.stream()
                .filter(e -> e instanceof OrderBookSnapshotEvent).map(e -> (OrderBookSnapshotEvent) e)
                .collect(Collectors.groupingBy(b -> b.instrument().symbol()));
        assertThat(books.keySet()).as("instruments with books within 30s").containsExactlyInAnyOrderElementsOf(SYMBOLS);
        assertThat(meters.counter("muninn.ingest.source.unusable.messages", "source", "binance.spot.v1").count())
                .as("messages that could not be attributed or parsed").isZero();
        // G0.18b: one socket, so each pair's trades and books each count 1, 2, 3 with no repeats.
        SequenceSeries.assertEachCountsFromOneWithoutGaps(events);

        Instant now = Instant.now();
        books.forEach((symbol, series) -> {
            BigDecimal median = series.stream().map(BinanceLiveSmokeTest::mid).sorted()
                    .toList().get(series.size() / 2);
            for (OrderBookSnapshotEvent b : series) {
                assertThat(b.source()).isEqualTo("binance.spot.v1");
                assertThat(b.bids().getFirst().price()).isLessThan(b.asks().getFirst().price());
                // Within 5% of its own median over a few seconds: no other pair's book is mixed in.
                BigDecimal drift = mid(b).subtract(median).abs().divide(median, 6, RoundingMode.HALF_UP);
                assertThat(drift).as("%s book at %s against median %s", symbol, mid(b), median)
                        .isLessThan(new BigDecimal("0.05"));
                assertThat(Duration.between(b.ingestTime(), now).abs()).isLessThan(Duration.ofMinutes(2));
            }
        });
        for (MarketEvent e : events) {
            if (e instanceof TradeEvent t) {
                assertThat(SYMBOLS).contains(t.instrument().symbol());
                assertThat(Duration.between(t.eventTime(), now).abs()).isLessThan(Duration.ofMinutes(2));
            }
        }
        System.out.printf("Binance live: %s%n", books.entrySet().stream()
                .map(en -> en.getKey() + " " + en.getValue().size() + " books, mid " + mid(en.getValue().getFirst()))
                .collect(Collectors.joining("; ")));
    }

    private static boolean tenBooksForEveryPair(List<MarketEvent> events) {
        Map<String, Long> counts = events.stream().filter(e -> e instanceof OrderBookSnapshotEvent)
                .collect(Collectors.groupingBy(e -> e.instrument().symbol(), Collectors.counting()));
        return SYMBOLS.stream().allMatch(s -> counts.getOrDefault(s, 0L) >= 10);
    }

    private static BigDecimal mid(OrderBookSnapshotEvent b) {
        return b.bids().getFirst().price().add(b.asks().getFirst().price())
                .divide(BigDecimal.valueOf(2), 10, RoundingMode.HALF_UP);
    }
}
