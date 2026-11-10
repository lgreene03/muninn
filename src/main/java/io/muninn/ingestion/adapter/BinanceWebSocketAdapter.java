package io.muninn.ingestion.adapter;

import io.muninn.shared.event.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Binance public WebSocket adapter for trade and order-book streams.
 *
 * <p>Connects to Binance's combined streams endpoint for every configured instrument,
 * parses messages into canonical {@link MarketEvent}s, and delivers them to the
 * registered handler. Which streams are subscribed, and which instrument each message
 * belongs to, is decided by {@link BinanceCombinedStream}, which is pure and tested on
 * its own; this class owns only the socket. Implements reconnection with exponential
 * backoff and jitter.</p>
 *
 * <p>Uses Java 21's built-in {@link java.net.http.HttpClient} WebSocket support —
 * no external WebSocket library needed.</p>
 */
public class BinanceWebSocketAdapter implements ExchangeAdapter {

    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketAdapter.class);
    private static final String SOURCE = BinanceCombinedStream.SOURCE;

    private final BinanceConfig config;
    private final BinanceCombinedStream streams;
    private final Counter reconnectsCounter;
    private final Counter unusableMessages;
    private final HttpClient httpClient;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile WebSocket webSocket;
    private volatile Consumer<MarketEvent> eventHandler;

    public BinanceWebSocketAdapter(BinanceConfig config, MeterRegistry meterRegistry) {
        this.config = config;
        // Built here so a configuration Binance would refuse fails at start-up.
        this.streams = new BinanceCombinedStream(config.instruments(), config.streams());
        this.reconnectsCounter = Counter.builder("muninn.ingest.source.reconnects")
                .description("Source reconnection attempts")
                .tag("source", SOURCE)
                .register(meterRegistry);
        this.unusableMessages = Counter.builder("muninn.ingest.source.unusable.messages")
                .description("Messages that could not be turned into events")
                .tag("source", SOURCE)
                .register(meterRegistry);
        this.httpClient = AdapterHttpClients.newWebSocketClient(SOURCE);
    }

    @Override
    public void start(Consumer<MarketEvent> handler) {
        this.eventHandler = handler;
        running.set(true);

        log.atInfo()
                .addKeyValue("source", SOURCE)
                .addKeyValue("instruments", config.instruments())
                .addKeyValue("streams", config.streams())
                .log("Starting Binance WebSocket adapter");

        connect();
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.atInfo().addKeyValue("source", SOURCE).log("Stopping Binance WebSocket adapter");
            if (webSocket != null) {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown")
                        .exceptionally(ex -> {
                            log.atWarn()
                                    .addKeyValue("source", SOURCE)
                                    .addKeyValue("error", ex.getMessage())
                                    .log("Error closing WebSocket");
                            return null;
                        });
            }
        }
    }

    @Override
    public String source() {
        return SOURCE;
    }

    private void connect() {
        if (!running.get()) return;

        URI uri = URI.create(config.baseUrl() + streams.path());

        log.atInfo()
                .addKeyValue("source", SOURCE)
                .addKeyValue("uri", uri)
                .log("Connecting to Binance WebSocket");

        httpClient.newWebSocketBuilder()
                .buildAsync(uri, new BinanceWebSocketListener())
                .thenAccept(ws -> {
                    this.webSocket = ws;
                    log.atInfo()
                            .addKeyValue("source", SOURCE)
                            .log("Binance WebSocket connected");
                })
                .exceptionally(ex -> {
                    log.atError()
                            .setCause(ex)
                            .addKeyValue("source", SOURCE)
                            .addKeyValue("error", ex.getMessage())
                            .log("Failed to connect to Binance WebSocket");
                    scheduleReconnect(1);
                    return null;
                });
    }

    private void scheduleReconnect(int attempt) {
        if (!running.get()) return;

        reconnectsCounter.increment();
        long delay = calculateBackoff(attempt);
        log.atInfo()
                .addKeyValue("source", SOURCE)
                .addKeyValue("attempt", attempt)
                .addKeyValue("delayMs", delay)
                .log("Scheduling WebSocket reconnect");

        Thread.ofVirtual().name("binance-reconnect").start(() -> {
            try {
                Thread.sleep(delay);
                if (running.get()) {
                    connect();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private long calculateBackoff(int attempt) {
        long delay = Math.min(
                config.reconnect().initialDelayMs() * (1L << Math.min(attempt, 20)),
                config.reconnect().maxDelayMs()
        );
        if (config.reconnect().jitter()) {
            delay = (long) (delay * (0.5 + Math.random() * 0.5));
        }
        return delay;
    }

    /**
     * WebSocket listener that accumulates message fragments and dispatches complete messages.
     */
    private class BinanceWebSocketListener implements WebSocket.Listener {

        private final StringBuilder messageBuffer = new StringBuilder();
        private int reconnectAttempt = 0;

        @Override
        public void onOpen(WebSocket webSocket) {
            reconnectAttempt = 0;
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            messageBuffer.append(data);
            if (last) {
                String message = messageBuffer.toString();
                messageBuffer.setLength(0);
                processMessage(message);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.atInfo()
                    .addKeyValue("source", SOURCE)
                    .addKeyValue("statusCode", statusCode)
                    .addKeyValue("reason", reason)
                    .log("Binance WebSocket closed");
            scheduleReconnect(++reconnectAttempt);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            // setCause, not only the message as a key-value: the console pattern does not
            // print key-values, so for four months this line said only "error" while a
            // NoClassDefFoundError was the cause (see AdapterHttpClients).
            log.atError()
                    .setCause(error)
                    .addKeyValue("source", SOURCE)
                    .addKeyValue("error", error.getMessage())
                    .log("Binance WebSocket error");
            scheduleReconnect(++reconnectAttempt);
        }

        private void processMessage(String rawMessage) {
            Consumer<MarketEvent> handler = eventHandler;
            if (handler == null) return;

            BinanceCombinedStream.Result result = streams.route(rawMessage, Instant.now());
            if (result.problem() != null) {
                unusableMessages.increment();
                log.atWarn()
                        .addKeyValue("source", SOURCE)
                        .addKeyValue("problem", result.problem())
                        .log("Unusable Binance message: " + result.problem());
                return;
            }
            handler.accept(result.event());
        }
    }
}
