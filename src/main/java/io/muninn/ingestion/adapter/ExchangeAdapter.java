package io.muninn.ingestion.adapter;

import io.muninn.shared.event.MarketEvent;

import java.util.function.Consumer;

/**
 * Interface for exchange data source adapters.
 *
 * <p>An adapter connects to an exchange's public data feed (WebSocket, REST, etc.),
 * normalizes incoming data into canonical {@link MarketEvent}s, and delivers them
 * to a callback. The adapter owns reconnection logic, heartbeat handling, and
 * backpressure signaling.</p>
 *
 * <p>Adapters are {@link AutoCloseable} and must release all resources (connections,
 * threads, buffers) when closed.</p>
 */
public interface ExchangeAdapter extends AutoCloseable {

    /**
     * Start consuming events from the exchange. Events are delivered to the
     * provided handler on the adapter's internal thread(s). The handler must
     * be thread-safe if the adapter delivers from multiple threads.
     *
     * @param eventHandler callback for each normalized event
     */
    void start(Consumer<MarketEvent> eventHandler);

    /**
     * Stop consuming and release resources. Idempotent.
     */
    void stop();

    /**
     * The adapter's source identifier, e.g. {@code "binance.spot.v1"}.
     * Used as the {@code source} field on every emitted event.
     */
    String source();

    @Override
    default void close() {
        stop();
    }
}
