package io.muninn.ingestion.adapter;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Binance public WebSocket adapter for trade and order-book streams.
 *
 * <p>Connects to Binance's combined streams endpoint, parses messages into
 * canonical {@link MarketEvent}s, and delivers them to the registered handler.
 * Implements reconnection with exponential backoff and jitter.</p>
 *
 * <p>Uses Java 21's built-in {@link java.net.http.HttpClient} WebSocket support —
 * no external WebSocket library needed.</p>
 */
public class BinanceWebSocketAdapter implements ExchangeAdapter {

    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketAdapter.class);
    private static final String SOURCE = "binance.spot.v1";

    private static final Exchange BINANCE_EXCHANGE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));

    private final BinanceConfig config;
    private final HttpClient httpClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong tradeSequence = new AtomicLong(0);
    private final AtomicLong bookSequence = new AtomicLong(0);

    private volatile WebSocket webSocket;
    private volatile Consumer<MarketEvent> eventHandler;

    public BinanceWebSocketAdapter(BinanceConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
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

        String instrumentLower = config.instruments().getFirst().toLowerCase();
        StringBuilder streamPath = new StringBuilder("/stream?streams=");
        for (int i = 0; i < config.streams().size(); i++) {
            if (i > 0) streamPath.append("/");
            streamPath.append(instrumentLower).append("@").append(config.streams().get(i));
        }

        URI uri = URI.create(config.baseUrl() + streamPath);

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
                            .addKeyValue("source", SOURCE)
                            .addKeyValue("error", ex.getMessage())
                            .log("Failed to connect to Binance WebSocket");
                    scheduleReconnect(1);
                    return null;
                });
    }

    private void scheduleReconnect(int attempt) {
        if (!running.get()) return;

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

    private Instrument resolveInstrument(String binanceSymbol) {
        // MVP: single instrument, normalized to canonical naming
        String upper = binanceSymbol.toUpperCase();
        String base;
        String quote;
        if (upper.endsWith("USDT")) {
            base = upper.substring(0, upper.length() - 4);
            quote = "USDT";
        } else if (upper.endsWith("USD")) {
            base = upper.substring(0, upper.length() - 3);
            quote = "USD";
        } else {
            base = upper;
            quote = "UNKNOWN";
        }
        return new Instrument(base + "-" + quote, base, quote, BINANCE_EXCHANGE);
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
            log.atError()
                    .addKeyValue("source", SOURCE)
                    .addKeyValue("error", error.getMessage())
                    .log("Binance WebSocket error");
            scheduleReconnect(++reconnectAttempt);
        }

        private void processMessage(String rawMessage) {
            if (eventHandler == null) return;

            try {
                Instant ingestTime = Instant.now();
                // Binance combined stream wraps messages in {"stream":"...","data":{...}}
                var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                var node = mapper.readTree(rawMessage);

                String stream = node.has("stream") ? node.get("stream").asText() : "";
                var dataNode = node.has("data") ? node.get("data") : node;
                String data = mapper.writeValueAsString(dataNode);

                Instrument instrument = resolveInstrument(
                        config.instruments().getFirst()
                );

                if (stream.contains("@trade") || (dataNode.has("e") && "trade".equals(dataNode.get("e").asText()))) {
                    MarketEvent event = BinanceTradeParser.parse(
                            data, instrument, SOURCE, ingestTime, tradeSequence.incrementAndGet()
                    );
                    eventHandler.accept(event);
                } else if (stream.contains("@depth") || dataNode.has("bids")) {
                    MarketEvent event = BinanceBookParser.parse(
                            data, instrument, SOURCE, ingestTime, bookSequence.incrementAndGet()
                    );
                    eventHandler.accept(event);
                } else {
                    log.atDebug()
                            .addKeyValue("source", SOURCE)
                            .addKeyValue("stream", stream)
                            .log("Ignoring unknown stream message");
                }
            } catch (Exception e) {
                log.atWarn()
                        .addKeyValue("source", SOURCE)
                        .addKeyValue("error", e.getMessage())
                        .log("Failed to process WebSocket message");
            }
        }
    }
}
