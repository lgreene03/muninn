package io.muninn.ingestion;

import io.muninn.ingestion.adapter.BinanceConfig;
import io.muninn.ingestion.adapter.BinanceWebSocketAdapter;
import io.muninn.ingestion.adapter.ExchangeAdapter;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.validation.EventValidator;
import io.muninn.shared.validation.ValidationResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Orchestrates the ingestion pipeline: adapter → validator → producer (with dead-letter routing).
 *
 * <p>Starts the configured exchange adapter(s) when the Spring application is ready,
 * validates each incoming event, and routes valid events to their canonical Redpanda topic
 * or rejects them to the dead-letter topic with structured failure reasons.</p>
 */
@Component
public class IngestionPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipeline.class);

    private final BinanceConfig binanceConfig;
    private final MarketEventProducer eventProducer;
    private final DeadLetterProducer deadLetterProducer;
    private final EventValidator eventValidator;
    private final MeterRegistry meterRegistry;

    private ExchangeAdapter activeAdapter;
    private volatile Instant lastEventTime = Instant.now();

    // Metrics
    private Counter eventsIngestedCounter;
    private Counter eventsRejectedCounter;
    private Timer sourceLatencyTimer;

    public IngestionPipeline(BinanceConfig binanceConfig,
                             MarketEventProducer eventProducer,
                             DeadLetterProducer deadLetterProducer,
                             MeterRegistry meterRegistry) {
        this.binanceConfig = binanceConfig;
        this.eventProducer = eventProducer;
        this.deadLetterProducer = deadLetterProducer;
        this.meterRegistry = meterRegistry;
        this.eventValidator = new EventValidator();
        initializeMetrics();
    }

    private void initializeMetrics() {
        this.eventsIngestedCounter = Counter.builder("muninn.ingest.events.total")
                .description("Total events ingested")
                .tag("source", "binance.spot.v1")
                .register(meterRegistry);

        this.eventsRejectedCounter = Counter.builder("muninn.ingest.validation.failed")
                .description("Events rejected at validation")
                .tag("source", "binance.spot.v1")
                .register(meterRegistry);

        this.sourceLatencyTimer = Timer.builder("muninn.ingest.source.latency")
                .description("Latency between event time and ingest time")
                .tag("source", "binance.spot.v1")
                .register(meterRegistry);

        Gauge.builder("muninn.ingest.lag.seconds", () -> {
            return Duration.between(lastEventTime, Instant.now()).toSeconds();
        })
        .description("Wall-clock seconds since last event from source")
        .tag("source", "binance.spot.v1")
        .tag("instrument", "BTC-USDT")
        .register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!binanceConfig.enabled()) {
            log.atInfo().log("Binance adapter disabled — skipping ingestion start");
            return;
        }

        log.atInfo()
                .addKeyValue("source", "binance.spot.v1")
                .addKeyValue("instruments", binanceConfig.instruments())
                .log("Starting ingestion pipeline");

        this.activeAdapter = new BinanceWebSocketAdapter(binanceConfig, meterRegistry);
        activeAdapter.start(this::handleEvent);
    }

    /**
     * Process an incoming event: validate, measure, and route to the appropriate destination.
     */
    private void handleEvent(MarketEvent event) {
        // Record source latency
        Duration latency = Duration.between(event.eventTime(), Instant.now());
        sourceLatencyTimer.record(latency);

        // Validate
        ValidationResult result = eventValidator.validate(event);

        switch (result) {
            case ValidationResult.Valid valid -> {
                lastEventTime = event.eventTime();
                eventsIngestedCounter.increment();
                eventProducer.publish(event);

                log.atDebug()
                        .addKeyValue("eventId", event.eventId())
                        .addKeyValue("type", event.getClass().getSimpleName())
                        .addKeyValue("instrument", event.instrument().symbol())
                        .log("Event accepted");
            }
            case ValidationResult.Invalid invalid -> {
                eventsRejectedCounter.increment();
                deadLetterProducer.reject(event, String.join("; ", invalid.reasons()), event.source());

                log.atWarn()
                        .addKeyValue("eventId", event.eventId())
                        .addKeyValue("reasons", invalid.reasons())
                        .log("Event rejected at validation");
            }
        }
    }

    /**
     * Graceful shutdown — stop the adapter and release resources.
     */
    @jakarta.annotation.PreDestroy
    public void shutdown() {
        if (activeAdapter != null) {
            log.atInfo().log("Shutting down ingestion pipeline");
            activeAdapter.stop();
        }
    }
}
