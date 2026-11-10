package io.muninn.ingestion.adapter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;

/**
 * Reconnect decisions for a WebSocket adapter, kept apart from the socket so they can be
 * tested. Used by the OKX adapter, and by the Binance adapter since G0.18b. Written to
 * avoid two defects the Binance adapter had until then:
 *
 * <ul>
 *   <li><b>Backoff that never escalates.</b> Binance kept its attempt count on the
 *       listener and built a new listener for every connection, so every reconnect was
 *       attempt one. Here failures are counted for the adapter's lifetime and reset only
 *       when real data arrives, not when a socket merely opens: a server that accepts
 *       and immediately closes must not be retried at the minimum delay for ever.</li>
 *   <li><b>Two reconnects for one failure.</b> Binance scheduled a reconnect from both
 *       {@code onClose} and {@code onError}, so any two signals for one broken connection
 *       would have left two sockets running and every event duplicated. The JDK documents
 *       each of those callbacks as the last for its socket, but an adapter has other
 *       signals too (a failed connect, OKX's keepalive abort) and the guard should not
 *       rest on one library's contract. Here a reconnect is single-flight: while one is
 *       pending, further disconnect signals are ignored.</li>
 * </ul>
 */
final class ReconnectPolicy {

    /** Returned by {@link #onDisconnect} when a reconnect is already pending. */
    static final long ALREADY_PENDING = -1L;

    private final long initialDelayMs;
    private final long maxDelayMs;
    private final boolean jitter;
    private final DoubleSupplier random;

    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicBoolean pending = new AtomicBoolean();

    ReconnectPolicy(BinanceConfig.Reconnect config, DoubleSupplier random) {
        this.initialDelayMs = config.initialDelayMs();
        this.maxDelayMs = config.maxDelayMs();
        this.jitter = config.jitter();
        this.random = random;
    }

    /**
     * A connection was lost or could not be made.
     *
     * @return the delay before reconnecting, or {@link #ALREADY_PENDING} if a reconnect
     *         is already scheduled, in which case the caller must do nothing
     */
    long onDisconnect() {
        if (!pending.compareAndSet(false, true)) {
            return ALREADY_PENDING;
        }
        return delayFor(consecutiveFailures.incrementAndGet());
    }

    /** The scheduled reconnect has begun; the next failure may schedule another. */
    void onReconnectStarted() {
        pending.set(false);
    }

    /** Real data arrived, so the connection works and backoff starts again from the bottom. */
    void onHealthy() {
        consecutiveFailures.set(0);
    }

    int consecutiveFailures() {
        return consecutiveFailures.get();
    }

    long delayFor(int failures) {
        int shift = Math.min(Math.max(failures - 1, 0), 20);
        long delay = Math.min(initialDelayMs << shift, maxDelayMs);
        if (jitter) {
            // Full jitter over the upper half: spreads a fleet's reconnects without ever
            // dropping below half the intended delay.
            delay = (long) (delay * (0.5 + random.getAsDouble() * 0.5));
        }
        return delay;
    }
}
