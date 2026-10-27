package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.TradeEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Runs the real adapter against OKX's live public feed. Off by default, so CI never
 * depends on an external network or a venue's uptime:
 *
 * <pre>mvn test -Dtest=OkxLiveSmokeTest -Dmuninn.live=true</pre>
 *
 * <p>Public data only. Proves what the fixture tests cannot: that the socket connects,
 * the subscription is accepted, and real traffic arrives as well-formed events.
 */
@EnabledIfSystemProperty(named = "muninn.live", matches = "true")
class OkxLiveSmokeTest {

    @Test
    void realTradesAndBooksArriveAsWellFormedEvents() throws Exception {
        List<MarketEvent> events = new CopyOnWriteArrayList<>();
        OkxWebSocketAdapter adapter = new OkxWebSocketAdapter(
                new OkxConfig(true, null, List.of("BTC-USDT"), null, null), new SimpleMeterRegistry());
        adapter.start(events::add);
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline
                    && (events.stream().noneMatch(e -> e instanceof TradeEvent)
                        || events.stream().filter(e -> e instanceof OrderBookSnapshotEvent).count() < 10)) {
                Thread.sleep(200);
            }
        } finally {
            adapter.stop();
        }

        List<OrderBookSnapshotEvent> books = events.stream()
                .filter(e -> e instanceof OrderBookSnapshotEvent).map(e -> (OrderBookSnapshotEvent) e).toList();
        List<TradeEvent> trades = events.stream()
                .filter(e -> e instanceof TradeEvent).map(e -> (TradeEvent) e).toList();
        assertThat(books).as("books5 snapshots within 30s").hasSizeGreaterThanOrEqualTo(10);
        assertThat(trades).as("at least one trade within 30s").isNotEmpty();

        Instant now = Instant.now();
        for (OrderBookSnapshotEvent b : books) {
            assertThat(b.instrument().exchange().id()).isEqualTo("okx");
            assertThat(b.bids()).hasSizeBetween(1, 5);
            assertThat(b.asks()).hasSizeBetween(1, 5);
            assertThat(b.bids().getFirst().price()).isLessThan(b.asks().getFirst().price());
            // Exchange time, and close to now: catches a units mistake (ms read as s or µs).
            assertThat(Duration.between(b.eventTime(), now).abs()).isLessThan(Duration.ofMinutes(2));
        }
        for (TradeEvent t : trades) {
            assertThat(t.price().signum()).isPositive();
            assertThat(Duration.between(t.eventTime(), now).abs()).isLessThan(Duration.ofMinutes(2));
        }
        System.out.printf("OKX live: %d books, %d trades; first book bid %s / ask %s%n", books.size(),
                trades.size(), books.getFirst().bids().getFirst().price(), books.getFirst().asks().getFirst().price());
    }
}
