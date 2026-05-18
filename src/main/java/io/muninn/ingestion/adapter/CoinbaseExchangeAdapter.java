package io.muninn.ingestion.adapter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.muninn.shared.event.MarketEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Coinbase Pro reference exchange WebSocket adapter.
 *
 * <p>Demonstrates how secondary or alternative exchanges are plugged into the
 * multi-exchange ingestion adapter framework in Phase 8 without modifying core pipeline code.</p>
 */
public class CoinbaseExchangeAdapter implements ExchangeAdapter {

    private static final Logger log = LoggerFactory.getLogger(CoinbaseExchangeAdapter.class);
    private static final String SOURCE = "coinbase.pro.v1";

    private final CoinbaseConfig config;
    private final MeterRegistry meterRegistry;
    private final Counter reconnectsCounter;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;

    private volatile Consumer<MarketEvent> eventHandler;

    /**
     * Single-arg constructor retained for tests that don't exercise config.
     * Equivalent to passing a default {@link CoinbaseConfig}.
     */
    public CoinbaseExchangeAdapter(MeterRegistry meterRegistry) {
        this(new CoinbaseConfig(false, null, null, null), meterRegistry);
    }

    public CoinbaseExchangeAdapter(CoinbaseConfig config, MeterRegistry meterRegistry) {
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.reconnectsCounter = Counter.builder("muninn.ingest.source.reconnects")
                .description("Source reconnection attempts")
                .tag("source", SOURCE)
                .register(meterRegistry);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "coinbase-adapter-worker");
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void start(Consumer<MarketEvent> handler) {
        this.eventHandler = handler;
        if (running.compareAndSet(false, true)) {
            log.atInfo()
                    .addKeyValue("source", SOURCE)
                    .log("Starting Coinbase Exchange Adapter reference stream");
            
            // Simulate periodic ingestion pulses / heartbeats for testing and demo purposes
            scheduler.scheduleAtFixedRate(this::tickHeartbeat, 5, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.atInfo()
                    .addKeyValue("source", SOURCE)
                    .log("Stopping Coinbase Exchange Adapter");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public String source() {
        return SOURCE;
    }

    private void tickHeartbeat() {
        if (!running.get()) return;
        
        log.atDebug()
                .addKeyValue("source", SOURCE)
                .log("Coinbase heartbeat check ok");
    }
}
