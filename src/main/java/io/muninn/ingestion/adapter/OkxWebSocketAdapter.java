package io.muninn.ingestion.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.muninn.shared.event.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * OKX public market data: trades and five-level book snapshots, for every configured
 * spot instrument. No API key, no account, no trading endpoint.
 *
 * <p>Message handling lives in {@link OkxMessageParser} and reconnect decisions in
 * {@link ReconnectPolicy}, both pure and tested on their own. This class owns only the
 * socket. Three properties matter here:
 *
 * <ul>
 *   <li><b>Every configured instrument is subscribed.</b> The Binance adapter uses only
 *       the first.</li>
 *   <li><b>A stale socket cannot disturb a live one.</b> Each connection has a
 *       generation, and close or error signals from an older generation are ignored.</li>
 *   <li><b>Keepalive.</b> OKX closes a connection after 30 seconds without traffic. The
 *       adapter sends {@code ping} after 20 seconds of silence and, if still nothing has
 *       arrived by 30, abandons the socket and reconnects rather than waiting on a
 *       connection the venue has already given up on.</li>
 * </ul>
 */
public class OkxWebSocketAdapter implements ExchangeAdapter {

    private static final Logger log = LoggerFactory.getLogger(OkxWebSocketAdapter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration PING_AFTER = Duration.ofSeconds(20);
    private static final Duration ABANDON_AFTER = Duration.ofSeconds(30);

    private final OkxConfig config;
    private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final ReconnectPolicy reconnect;
    private final OkxMessageParser parser;
    private final Counter reconnects;
    private final Counter problems;
    private final Counter venueErrors;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong generation = new AtomicLong();
    private final AtomicLong lastMessageAtMs = new AtomicLong();
    private final AtomicBoolean pingOutstanding = new AtomicBoolean();
    private volatile WebSocket webSocket;
    private volatile Consumer<MarketEvent> eventHandler;
    private volatile Thread keepalive;

    public OkxWebSocketAdapter(OkxConfig config, MeterRegistry meters) {
        this.config = config;
        this.reconnect = new ReconnectPolicy(config.reconnect(), () -> ThreadLocalRandom.current().nextDouble());
        AtomicLong tradeSequence = new AtomicLong();
        AtomicLong bookSequence = new AtomicLong();
        this.parser = new OkxMessageParser(tradeSequence::incrementAndGet, bookSequence::incrementAndGet);
        this.reconnects = Counter.builder("muninn.ingest.source.reconnects")
                .tag("source", OkxMessageParser.SOURCE).register(meters);
        this.problems = Counter.builder("muninn.ingest.source.unusable.messages")
                .description("Messages that could not be turned into events")
                .tag("source", OkxMessageParser.SOURCE).register(meters);
        this.venueErrors = Counter.builder("muninn.ingest.source.venue.errors")
                .tag("source", OkxMessageParser.SOURCE).register(meters);
    }

    @Override
    public void start(Consumer<MarketEvent> handler) {
        this.eventHandler = handler;
        if (!running.compareAndSet(false, true)) {
            return;
        }
        log.atInfo().addKeyValue("source", source()).addKeyValue("instruments", config.instruments())
                .addKeyValue("channels", config.channels()).log("Starting OKX adapter");
        keepalive = Thread.ofVirtual().name("okx-keepalive").start(this::keepaliveLoop);
        connect();
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.atInfo().addKeyValue("source", source()).log("Stopping OKX adapter");
            if (keepalive != null) {
                keepalive.interrupt();
            }
            WebSocket ws = webSocket;
            if (ws != null) {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").exceptionally(ex -> null);
            }
        }
    }

    @Override
    public String source() {
        return OkxMessageParser.SOURCE;
    }

    private void connect() {
        if (!running.get()) {
            return;
        }
        long gen = generation.incrementAndGet();
        lastMessageAtMs.set(System.currentTimeMillis());
        pingOutstanding.set(false);
        httpClient.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .buildAsync(URI.create(config.url()), new Listener(gen))
                .thenAccept(ws -> webSocket = ws)
                .exceptionally(ex -> {
                    log.atWarn().addKeyValue("source", source()).addKeyValue("error", ex.getMessage())
                            .log("OKX connect failed");
                    disconnected(gen);
                    return null;
                });
    }

    /** One reconnect per failure, however many signals report it. */
    private void disconnected(long gen) {
        if (!running.get() || gen != generation.get()) {
            return; // shutting down, or a stale socket's late signal
        }
        long delay = reconnect.onDisconnect();
        if (delay == ReconnectPolicy.ALREADY_PENDING) {
            return;
        }
        reconnects.increment();
        log.atInfo().addKeyValue("source", source()).addKeyValue("failures", reconnect.consecutiveFailures())
                .addKeyValue("delayMs", delay).log("Scheduling OKX reconnect");
        Thread.ofVirtual().name("okx-reconnect").start(() -> {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            reconnect.onReconnectStarted();
            connect();
        });
    }

    private void keepaliveLoop() {
        while (running.get()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                return;
            }
            long silentMs = System.currentTimeMillis() - lastMessageAtMs.get();
            WebSocket ws = webSocket;
            if (ws == null) {
                continue;
            }
            if (silentMs >= ABANDON_AFTER.toMillis()) {
                log.atWarn().addKeyValue("source", source()).addKeyValue("silentMs", silentMs)
                        .log("OKX connection silent; abandoning it and reconnecting");
                webSocket = null; // so this loop does not re-abandon it during the backoff
                ws.abort();
                disconnected(generation.get());
            } else if (silentMs >= PING_AFTER.toMillis() && pingOutstanding.compareAndSet(false, true)) {
                ws.sendText("ping", true);
            }
        }
    }

    private String subscribeMessage() {
        ObjectNode root = MAPPER.createObjectNode().put("op", "subscribe");
        ArrayNode args = root.putArray("args");
        for (String instrument : config.instruments()) {
            for (String channel : config.channels()) {
                args.addObject().put("channel", channel).put("instId", instrument);
            }
        }
        return root.toString();
    }

    private void handle(String message) {
        lastMessageAtMs.set(System.currentTimeMillis());
        pingOutstanding.set(false);
        OkxMessageParser.Result result = parser.parse(message, Instant.now());
        if (result.problem() != null) {
            if (result.venueError()) {
                venueErrors.increment();
                log.atError().addKeyValue("source", source()).addKeyValue("problem", result.problem())
                        .log("OKX reported an error");
            } else {
                problems.increment();
                log.atWarn().addKeyValue("source", source()).addKeyValue("problem", result.problem())
                        .log("Unusable OKX message");
            }
        }
        if (!result.events().isEmpty()) {
            reconnect.onHealthy();
            Consumer<MarketEvent> handler = eventHandler;
            if (handler != null) {
                result.events().forEach(handler);
            }
        }
    }

    private final class Listener implements WebSocket.Listener {
        private final long gen;
        private final StringBuilder buffer = new StringBuilder();

        Listener(long gen) {
            this.gen = gen;
        }

        @Override
        public void onOpen(WebSocket ws) {
            log.atInfo().addKeyValue("source", source()).log("OKX connected; subscribing");
            ws.sendText(subscribeMessage(), true);
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                String message = buffer.toString();
                buffer.setLength(0);
                if (gen == generation.get()) {
                    handle(message);
                }
            }
            ws.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            log.atInfo().addKeyValue("source", source()).addKeyValue("statusCode", statusCode)
                    .addKeyValue("reason", reason).log("OKX connection closed");
            disconnected(gen);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            log.atWarn().addKeyValue("source", source()).addKeyValue("error", String.valueOf(error.getMessage()))
                    .log("OKX connection error");
            disconnected(gen);
        }
    }
}
