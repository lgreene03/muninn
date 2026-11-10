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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Binance public WebSocket adapter for trade and order-book streams.
 *
 * <p>Connects to Binance's combined streams endpoint for every configured instrument,
 * parses messages into canonical {@link MarketEvent}s, and delivers them to the
 * registered handler. Which streams are subscribed, and which instrument each message
 * belongs to, is decided by {@link BinanceCombinedStream}, and when to reconnect by
 * {@link ReconnectPolicy}; both are pure and tested on their own. This class owns only
 * the socket.</p>
 *
 * <p>Since G0.18b it reconnects the way the OKX adapter does. A second live socket would
 * not show in the recording: both would feed the same per-pair sequence counters, so
 * every event would be recorded twice with clean, unbroken sequence numbers, and Binance
 * books carry no id to deduplicate them afterwards. So:</p>
 *
 * <ul>
 *   <li><b>One reconnect per failure.</b> However many close or error signals report a
 *       lost connection, one reconnect is scheduled; and a second {@code start} opens no
 *       second socket.</li>
 *   <li><b>Backoff escalates.</b> Failures are counted for the adapter's lifetime, not
 *       per connection, and the count resets only when an event arrives. A venue that
 *       refuses or drops every connection is retried after 1, 2, 4 seconds and so on up
 *       to the configured maximum, with jitter, rather than every second or two for
 *       ever.</li>
 *   <li><b>A stale socket cannot deliver.</b> Each connection has a generation. Messages
 *       and signals from an older one are ignored, and the older socket is aborted.</li>
 * </ul>
 *
 * <p>Uses Java 21's built-in {@link java.net.http.HttpClient} WebSocket support, so no
 * external WebSocket library is needed.</p>
 */
public class BinanceWebSocketAdapter implements ExchangeAdapter {

    private static final Logger log = LoggerFactory.getLogger(BinanceWebSocketAdapter.class);
    private static final String SOURCE = BinanceCombinedStream.SOURCE;

    /** Opens one socket. The seam tests use in place of the network. */
    @FunctionalInterface
    interface Connector {
        CompletableFuture<WebSocket> connect(URI uri, WebSocket.Listener listener);
    }

    /** Runs a task after a delay. The seam tests use in place of the clock. */
    @FunctionalInterface
    interface Scheduler {
        void schedule(long delayMs, Runnable task);
    }

    private final BinanceConfig config;
    private final BinanceCombinedStream streams;
    private final ReconnectPolicy reconnect;
    private final Connector connector;
    private final Scheduler scheduler;
    private final Counter reconnectsCounter;
    private final Counter unusableMessages;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong generation = new AtomicLong();

    private volatile WebSocket webSocket;
    private volatile Consumer<MarketEvent> eventHandler;

    public BinanceWebSocketAdapter(BinanceConfig config, MeterRegistry meterRegistry) {
        this(config, meterRegistry, overTheNetwork(AdapterHttpClients.newWebSocketClient(SOURCE)),
                BinanceWebSocketAdapter::sleepThenRun);
    }

    BinanceWebSocketAdapter(BinanceConfig config, MeterRegistry meterRegistry, Connector connector,
                            Scheduler scheduler) {
        this.config = config;
        // Built here so a configuration Binance would refuse fails at start-up.
        this.streams = new BinanceCombinedStream(config.instruments(), config.streams());
        this.reconnect = new ReconnectPolicy(config.reconnect(), () -> ThreadLocalRandom.current().nextDouble());
        this.connector = connector;
        this.scheduler = scheduler;
        this.reconnectsCounter = Counter.builder("muninn.ingest.source.reconnects")
                .description("Source reconnection attempts")
                .tag("source", SOURCE)
                .register(meterRegistry);
        this.unusableMessages = Counter.builder("muninn.ingest.source.unusable.messages")
                .description("Messages that could not be turned into events")
                .tag("source", SOURCE)
                .register(meterRegistry);
    }

    private static Connector overTheNetwork(HttpClient httpClient) {
        return (uri, listener) -> httpClient.newWebSocketBuilder().buildAsync(uri, listener);
    }

    private static void sleepThenRun(long delayMs, Runnable task) {
        Thread.ofVirtual().name("binance-reconnect").start(() -> {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            task.run();
        });
    }

    @Override
    public void start(Consumer<MarketEvent> handler) {
        this.eventHandler = handler;
        if (!running.compareAndSet(false, true)) {
            return; // already running: a second start must not open a second socket
        }

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
            WebSocket ws = webSocket;
            if (ws != null) {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown")
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

        long gen = generation.incrementAndGet();
        URI uri = URI.create(config.baseUrl() + streams.path());

        log.atInfo()
                .addKeyValue("source", SOURCE)
                .addKeyValue("uri", uri)
                .addKeyValue("generation", gen)
                .log("Connecting to Binance WebSocket");

        connector.connect(uri, new BinanceWebSocketListener(gen))
                .thenAccept(ws -> {
                    this.webSocket = ws;
                    log.atInfo()
                            .addKeyValue("source", SOURCE)
                            .addKeyValue("generation", gen)
                            .log("Binance WebSocket connected");
                })
                .exceptionally(ex -> {
                    log.atError()
                            .setCause(ex)
                            .addKeyValue("source", SOURCE)
                            .addKeyValue("error", ex.getMessage())
                            .log("Failed to connect to Binance WebSocket");
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
        reconnectsCounter.increment();
        log.atInfo()
                .addKeyValue("source", SOURCE)
                .addKeyValue("failures", reconnect.consecutiveFailures())
                .addKeyValue("delayMs", delay)
                .log("Scheduling WebSocket reconnect");
        scheduler.schedule(delay, () -> {
            reconnect.onReconnectStarted();
            connect();
        });
    }

    /**
     * WebSocket listener for one connection generation. Accumulates message fragments
     * and dispatches complete messages while its generation is the current one.
     */
    private final class BinanceWebSocketListener implements WebSocket.Listener {

        private final long gen;
        private final StringBuilder messageBuffer = new StringBuilder();

        BinanceWebSocketListener(long gen) {
            this.gen = gen;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if (gen != generation.get()) {
                // A socket from an earlier connection: nothing it carries is recorded,
                // and it is not asked for more.
                webSocket.abort();
                return null;
            }
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
                    .addKeyValue("generation", gen)
                    .log("Binance WebSocket closed");
            disconnected(gen);
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
                    .addKeyValue("generation", gen)
                    .log("Binance WebSocket error");
            disconnected(gen);
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
            // An event, not merely an open socket, is the evidence the connection works.
            reconnect.onHealthy();
            handler.accept(result.event());
        }
    }
}
