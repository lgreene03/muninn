package io.muninn.ingestion;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.muninn.ingestion.adapter.ExchangeAdapter;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.validation.EventValidator;
import io.muninn.shared.validation.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Orchestrates the ingestion pipeline: adapters → validator → producer (with
 * dead-letter routing).
 *
 * <p>Drives every {@link ExchangeAdapter} bean Spring wired in — one per
 * enabled exchange, selected by per-exchange {@code muninn.ingestion.&lt;name&gt;.enabled}
 * flags via {@code IngestionAdapterConfiguration}. Each adapter delivers
 * canonical {@link MarketEvent}s to the same handler, which validates and
 * routes them to the canonical Redpanda topic or dead-letters them.</p>
 *
 * <p>Metrics are tagged per source — {@code muninn.ingest.events.total},
 * {@code muninn.ingest.validation.failed}, {@code muninn.ingest.source.latency},
 * {@code muninn.ingest.lag.seconds} all carry a {@code source} label matching
 * the adapter's {@link ExchangeAdapter#source()}. Dashboards can show
 * per-exchange health.</p>
 *
 * <p>See ADR-0008 for the multi-exchange framework design.</p>
 */
@Component
public class IngestionPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipeline.class);

    private final List<ExchangeAdapter> adapters;
    private final MarketEventProducer eventProducer;
    private final DeadLetterProducer deadLetterProducer;
    private final EventValidator eventValidator;
    private final MeterRegistry meterRegistry;

    /** Per-source mutable state. Keyed by adapter {@link ExchangeAdapter#source()}. */
    private final ConcurrentMap<String, Counter> ingestedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> rejectedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> latencyTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Instant> lastEventTimes = new ConcurrentHashMap<>();

    public IngestionPipeline(List<ExchangeAdapter> adapters,
                             MarketEventProducer eventProducer,
                             DeadLetterProducer deadLetterProducer,
                             MeterRegistry meterRegistry) {
        this.adapters = List.copyOf(adapters);
        this.eventProducer = eventProducer;
        this.deadLetterProducer = deadLetterProducer;
        this.meterRegistry = meterRegistry;
        this.eventValidator = new EventValidator();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (adapters.isEmpty()) {
            log.atInfo().log("No exchange adapters enabled — ingestion pipeline idle");
            return;
        }

        log.atInfo()
                .addKeyValue("adapterCount", adapters.size())
                .addKeyValue("sources", adapters.stream().map(ExchangeAdapter::source).toList())
                .log("Starting ingestion pipeline");

        for (ExchangeAdapter adapter : adapters) {
            registerSourceMetrics(adapter.source());
            adapter.start(this::handleEvent);
        }
    }

    /** Initialize per-source metric instances on registration so the timeseries exist at t=0. */
    private void registerSourceMetrics(String source) {
        ingestedCounters.computeIfAbsent(source, src ->
                Counter.builder("muninn.ingest.events.total")
                        .description("Total events ingested")
                        .tag("source", src)
                        .register(meterRegistry));

        rejectedCounters.computeIfAbsent(source, src ->
                Counter.builder("muninn.ingest.validation.failed")
                        .description("Events rejected at validation")
                        .tag("source", src)
                        .register(meterRegistry));

        latencyTimers.computeIfAbsent(source, src ->
                Timer.builder("muninn.ingest.source.latency")
                        .description("Latency between event time and ingest time")
                        .tag("source", src)
                        .register(meterRegistry));

        // Initialize the lag clock at registration so a fresh source doesn't
        // read as "infinitely behind" before its first event arrives.
        lastEventTimes.put(source, Instant.now());
        Gauge.builder("muninn.ingest.lag.seconds",
                        () -> Duration.between(
                                lastEventTimes.getOrDefault(source, Instant.now()),
                                Instant.now()).toSeconds())
                .description("Wall-clock seconds since last event from source")
                .tag("source", source)
                .register(meterRegistry);
    }

    /**
     * Process one event: record metrics tagged by its source, validate, route.
     *
     * <p>Called on the adapter's internal thread. The handler is shared across
     * all adapters but each call references the event's own {@code source}, so
     * there's no cross-source state leakage even if multiple adapters call
     * concurrently.</p>
     */
    private void handleEvent(MarketEvent event) {
        String source = event.source();

        // Record source latency.
        Duration latency = Duration.between(event.eventTime(), Instant.now());
        Timer timer = latencyTimers.get(source);
        if (timer != null) timer.record(latency);

        ValidationResult result = eventValidator.validate(event);

        switch (result) {
            case ValidationResult.Valid valid -> {
                lastEventTimes.put(source, event.eventTime());
                Counter ingested = ingestedCounters.get(source);
                if (ingested != null) ingested.increment();
                eventProducer.publish(event);

                log.atDebug()
                        .addKeyValue("source", source)
                        .addKeyValue("eventId", event.eventId())
                        .addKeyValue("type", event.getClass().getSimpleName())
                        .addKeyValue("instrument", event.instrument().symbol())
                        .log("Event accepted");
            }
            case ValidationResult.Invalid invalid -> {
                Counter rejected = rejectedCounters.get(source);
                if (rejected != null) rejected.increment();
                deadLetterProducer.reject(event, String.join("; ", invalid.reasons()), source);

                log.atWarn()
                        .addKeyValue("source", source)
                        .addKeyValue("eventId", event.eventId())
                        .addKeyValue("reasons", invalid.reasons())
                        .log("Event rejected at validation");
            }
        }
    }

    /** Test hook: number of adapters Spring wired. */
    public int activeAdapterCount() {
        return adapters.size();
    }

    /**
     * Graceful shutdown — stop every active adapter and release resources.
     */
    @jakarta.annotation.PreDestroy
    public void shutdown() {
        if (adapters.isEmpty()) return;
        log.atInfo()
                .addKeyValue("adapterCount", adapters.size())
                .log("Shutting down ingestion pipeline");
        for (ExchangeAdapter adapter : adapters) {
            try {
                adapter.stop();
            } catch (Exception e) {
                log.atWarn().setCause(e)
                        .addKeyValue("source", adapter.source())
                        .log("Adapter shutdown raised; continuing");
            }
        }
    }
}
