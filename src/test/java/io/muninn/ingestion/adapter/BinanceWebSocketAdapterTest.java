package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import java.io.IOException;
import java.net.URI;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * The adapter's reconnect behaviour, driven through its two seams: the network (which
 * socket opens, and when) and the clock (when a scheduled reconnect runs). The listener
 * is the adapter's own, called through the JDK's {@link WebSocket.Listener} interface,
 * so every close, error and message can be fired in a chosen order.
 *
 * <p>Why it matters for the recording: a second live socket would feed the same per-pair
 * sequence counters as the first, so every event would be recorded twice with clean,
 * unbroken sequence numbers, and Binance books carry no id to deduplicate them
 * afterwards. Until G0.18b the adapter scheduled a reconnect from both {@code onClose}
 * and {@code onError}, kept its attempt count on a per-connection listener so backoff
 * never escalated, and delivered whatever any socket it had ever opened sent it.
 */
class BinanceWebSocketAdapterTest {

    private static final BinanceConfig CONFIG = new BinanceConfig(true, "wss://binance.invalid",
            List.of("btcusdt", "ethusdt"), List.of("trade", "depth20@100ms"),
            new BinanceConfig.Reconnect(1000, 60_000, false));

    private final FakeNetwork network = new FakeNetwork();
    private final FakeClock clock = new FakeClock();
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final List<MarketEvent> events = new ArrayList<>();
    private final BinanceWebSocketAdapter adapter = new BinanceWebSocketAdapter(CONFIG, meters, network, clock);

    @Test
    void aCloseAndAnErrorForOneConnectionScheduleOneReconnect() {
        adapter.start(events::add);
        Connection first = network.last();
        first.open();

        first.close();
        first.error();

        assertThat(clock.delays).as("reconnects scheduled").hasSize(1);
        assertThat(reconnects()).as("reconnects counted").isEqualTo(1.0);
        clock.runDue();
        assertThat(network.connections).as("sockets opened, the first included").hasSize(2);
    }

    @Test
    void startingTwiceOpensOneSocket() {
        adapter.start(events::add);
        adapter.start(events::add);

        assertThat(network.connections).hasSize(1);
    }

    @Test
    void backoffEscalatesAcrossConnectionsThatNeverDeliverAnEvent() {
        // A venue that accepts and at once drops every connection must not be retried at
        // the minimum delay for ever: merely opening is no evidence the connection works.
        adapter.start(events::add);
        for (int i = 0; i < 4; i++) {
            Connection c = network.last();
            c.open();
            c.close();
            clock.runDue();
        }

        assertThat(clock.delays).containsExactly(1000L, 2000L, 4000L, 8000L);
    }

    @Test
    void backoffEscalatesWhileTheVenueRefusesEveryConnection() {
        network.refuseConnections = true;
        adapter.start(events::add);
        clock.runDue();
        clock.runDue();
        clock.runDue();

        assertThat(clock.delays).containsExactly(1000L, 2000L, 4000L, 8000L);
        assertThat(network.connections).hasSize(4);
    }

    @Test
    void onlyAnEventResetsTheBackoffNotAnUnusableMessage() {
        adapter.start(events::add);
        Connection first = network.last();
        first.open();
        first.close();
        clock.runDue();

        Connection second = network.last();
        second.open();
        second.text("{\"result\":null,\"id\":1}"); // not a combined stream message: no event
        second.close();
        clock.runDue();

        Connection third = network.last();
        third.open();
        third.text(trade("BTCUSDT", 1));
        third.close();

        assertThat(clock.delays).containsExactly(1000L, 2000L, 1000L);
    }

    @Test
    void aStaleSocketFromAnEarlierConnectionCannotDeliverEvents() {
        adapter.start(events::add);
        Connection first = network.last();
        first.open();
        first.text(trade("BTCUSDT", 1));
        first.close();
        clock.runDue();
        Connection second = network.last();
        second.open();

        first.text(trade("BTCUSDT", 2));

        assertThat(events).as("events after the stale socket spoke").hasSize(1);
        assertThat(first.socket().aborted).as("the stale socket is abandoned").isTrue();

        second.text(trade("BTCUSDT", 3));

        assertThat(events).extracting(e -> ((TradeEvent) e).exchangeTradeId()).containsExactly("1", "3");
        // The pair's series runs on across the reconnect without a duplicate or a gap.
        assertThat(events).extracting(MarketEvent::sequenceNumber).containsExactly(1L, 2L);
    }

    @Test
    void aLateSignalFromAStaleSocketSchedulesNothing() {
        adapter.start(events::add);
        Connection first = network.last();
        first.open();
        first.close();
        clock.runDue();
        Connection second = network.last();
        second.open();

        first.error();

        assertThat(clock.delays).as("reconnects scheduled").hasSize(1);
        assertThat(clock.due).isEmpty();
        assertThat(network.connections).hasSize(2);
    }

    @Test
    void stoppingCancelsAPendingReconnectAndIgnoresLaterSignals() {
        adapter.start(events::add);
        Connection first = network.last();
        first.open();
        first.close();

        adapter.stop();
        clock.runDue();

        assertThat(network.connections).as("no socket opened after stop").hasSize(1);
        first.close();
        first.error();
        assertThat(clock.due).as("nothing scheduled after stop").isEmpty();
    }

    @Test
    void stopSendsAClose() {
        adapter.start(events::add);
        Connection first = network.last();
        first.open();

        adapter.stop();

        assertThat(first.socket().closeSent).isTrue();
    }

    private double reconnects() {
        return meters.counter("muninn.ingest.source.reconnects", "source", "binance.spot.v1").count();
    }

    private static String trade(String symbol, long tradeId) {
        return "{\"stream\":\"" + symbol.toLowerCase() + "@trade\",\"data\":{\"e\":\"trade\",\"E\":1762776000000,"
                + "\"s\":\"" + symbol + "\",\"t\":" + tradeId + ",\"p\":\"67000.10\",\"q\":\"0.5\","
                + "\"T\":1762776000000,\"m\":false,\"M\":true}}";
    }

    /** One socket the adapter asked for: the listener it supplied, and the socket it got. */
    private record Connection(URI uri, WebSocket.Listener listener, FakeSocket socket) {
        void open() {
            listener.onOpen(socket);
        }

        void text(String message) {
            listener.onText(socket, message, true);
        }

        void close() {
            listener.onClose(socket, 1006, "connection lost");
        }

        void error() {
            listener.onError(socket, new IOException("connection reset"));
        }
    }

    /** Records every socket the adapter opens; can refuse them all. */
    private static final class FakeNetwork implements BinanceWebSocketAdapter.Connector {
        final List<Connection> connections = new ArrayList<>();
        boolean refuseConnections;

        @Override
        public CompletableFuture<WebSocket> connect(URI uri, WebSocket.Listener listener) {
            Connection c = new Connection(uri, listener, new FakeSocket());
            connections.add(c);
            if (refuseConnections) {
                return CompletableFuture.failedFuture(new IOException("connection refused"));
            }
            return CompletableFuture.completedFuture(c.socket());
        }

        Connection last() {
            return connections.getLast();
        }
    }

    /** Holds each scheduled reconnect until the test runs it, and records its delay. */
    private static final class FakeClock implements BinanceWebSocketAdapter.Scheduler {
        final List<Long> delays = new ArrayList<>();
        final List<Runnable> due = new ArrayList<>();

        @Override
        public void schedule(long delayMs, Runnable task) {
            delays.add(delayMs);
            due.add(task);
        }

        /** Runs what is scheduled now; anything those tasks schedule waits for the next call. */
        void runDue() {
            List<Runnable> now = List.copyOf(due);
            due.clear();
            now.forEach(Runnable::run);
        }
    }

    private static final class FakeSocket implements WebSocket {
        boolean aborted;
        boolean closeSent;

        @Override
        public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) {
            return CompletableFuture.completedFuture(this);
        }

        @Override
        public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) {
            return CompletableFuture.completedFuture(this);
        }

        @Override
        public CompletableFuture<WebSocket> sendPing(ByteBuffer message) {
            return CompletableFuture.completedFuture(this);
        }

        @Override
        public CompletableFuture<WebSocket> sendPong(ByteBuffer message) {
            return CompletableFuture.completedFuture(this);
        }

        @Override
        public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) {
            closeSent = true;
            return CompletableFuture.completedFuture(this);
        }

        @Override
        public void request(long n) {
        }

        @Override
        public String getSubprotocol() {
            return "";
        }

        @Override
        public boolean isOutputClosed() {
            return closeSent || aborted;
        }

        @Override
        public boolean isInputClosed() {
            return aborted;
        }

        @Override
        public void abort() {
            aborted = true;
        }
    }
}
