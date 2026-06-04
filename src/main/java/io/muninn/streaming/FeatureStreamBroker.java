package io.muninn.streaming;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.muninn.shared.event.FeatureComputedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fan-out hub for the live feature stream. Holds the set of currently connected SSE clients and
 * broadcasts every {@link FeatureComputedEvent} the tail consumer reads to the subscribers whose
 * optional feature filter matches.
 *
 * <p>One process-wide broker serves all clients: the {@link FeatureStreamConsumer} reads each
 * feature topic once and the broker fans the event out in memory, so Kafka load is constant
 * regardless of how many dashboards connect.</p>
 *
 * <p>A scheduled keepalive sends an SSE comment to every open connection so idle reverse proxies do
 * not close the socket and dead clients are detected and pruned promptly.</p>
 */
public class FeatureStreamBroker implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(FeatureStreamBroker.class);

    private final Set<Subscription> subscriptions = ConcurrentHashMap.newKeySet();
    private final AtomicInteger active = new AtomicInteger();
    private final long emitterTimeoutMillis;
    private final ScheduledExecutorService keepaliveScheduler;

    private final Counter messagesSent;
    private final Counter disconnects;

    public FeatureStreamBroker(StreamingProperties properties, MeterRegistry meterRegistry) {
        this.emitterTimeoutMillis = properties.emitterTimeout().toMillis();

        Gauge.builder("muninn.streaming.subscriptions.active", active, AtomicInteger::get)
                .tag("endpoint", "features")
                .description("Currently connected SSE clients on the feature stream")
                .register(meterRegistry);
        this.messagesSent = Counter.builder("muninn.streaming.messages.sent")
                .tag("endpoint", "features")
                .description("Feature events successfully pushed to an SSE client")
                .register(meterRegistry);
        this.disconnects = Counter.builder("muninn.streaming.disconnects")
                .tag("endpoint", "features")
                .description("SSE clients removed on completion, timeout, or send failure")
                .register(meterRegistry);

        this.keepaliveScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "feature-stream-keepalive");
            thread.setDaemon(true);
            return thread;
        });
        Duration keepalive = properties.keepaliveInterval();
        this.keepaliveScheduler.scheduleAtFixedRate(
                this::sendKeepalives, keepalive.toMillis(), keepalive.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Register a new SSE client, optionally filtered to a single feature name.
     *
     * @param featureFilter a feature name to restrict the stream to, or {@code null}/blank for all
     * @return the emitter the controller returns to Spring MVC
     */
    public SseEmitter register(String featureFilter) {
        return registerEmitter(new SseEmitter(emitterTimeoutMillis), featureFilter);
    }

    // Package-private seam so tests can inject a mock emitter.
    SseEmitter registerEmitter(SseEmitter emitter, String featureFilter) {
        Subscription subscription =
                new Subscription(emitter, Optional.ofNullable(featureFilter).filter(s -> !s.isBlank()));
        subscriptions.add(subscription);
        active.incrementAndGet();
        emitter.onCompletion(() -> remove(subscription));
        emitter.onTimeout(() -> {
            remove(subscription);
            emitter.complete();
        });
        emitter.onError(error -> remove(subscription));
        log.atDebug()
                .addKeyValue("filter", featureFilter)
                .addKeyValue("active", active.get())
                .log("SSE client subscribed to feature stream");
        return emitter;
    }

    /** Broadcast one computed event to every subscriber whose filter matches. */
    public void broadcast(FeatureComputedEvent event) {
        for (Subscription subscription : subscriptions) {
            if (!subscription.matches(event)) {
                continue;
            }
            try {
                subscription.emitter().send(SseEmitter.event()
                        .name("feature")
                        .id(event.eventId().toString())
                        .data(event));
                messagesSent.increment();
            } catch (IOException | IllegalStateException e) {
                // Client went away (or the response was already committed/closed) — prune it.
                if (remove(subscription)) {
                    try {
                        subscription.emitter().completeWithError(e);
                    } catch (RuntimeException ignored) {
                        // Emitter was already completed by the container; nothing more to do.
                    }
                }
            }
        }
    }

    /** Visible for tests: the current number of connected clients. */
    int activeCount() {
        return active.get();
    }

    private void sendKeepalives() {
        for (Subscription subscription : subscriptions) {
            try {
                subscription.emitter().send(SseEmitter.event().comment("keepalive"));
            } catch (IOException | IllegalStateException e) {
                remove(subscription);
            }
        }
    }

    private boolean remove(Subscription subscription) {
        if (subscriptions.remove(subscription)) {
            active.decrementAndGet();
            disconnects.increment();
            return true;
        }
        return false;
    }

    @Override
    public void destroy() {
        keepaliveScheduler.shutdownNow();
        for (Subscription subscription : subscriptions) {
            subscription.emitter().complete();
        }
        subscriptions.clear();
        active.set(0);
    }

    /** A connected client and its optional single-feature filter. Identity is per-instance. */
    private record Subscription(SseEmitter emitter, Optional<String> featureFilter) {
        boolean matches(FeatureComputedEvent event) {
            return featureFilter.isEmpty() || featureFilter.get().equals(event.featureName());
        }
    }
}
