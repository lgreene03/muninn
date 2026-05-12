package io.muninn.feature.engine;

import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.feature.checkpoint.CheckpointState;
import io.muninn.feature.compute.VwapComputer;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * The main feature engine loop.
 *
 * <p>Consumes events from an {@link EventSource}, routes them through the
 * {@link WindowManager}, computes features when windows close, and emits
 * {@link FeatureComputedEvent}s to Kafka.</p>
 *
 * <p>This class is the orchestrator — it does not contain feature computation logic.
 * Computation is delegated to pure functions like {@link VwapComputer}.</p>
 *
 * <p>The engine runs in a single thread within the feature-engine loop. It is
 * started and stopped by Spring lifecycle management.</p>
 */
public final class FeatureEngineRunner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FeatureEngineRunner.class);

    public static final String MODE_LIVE = "live";
    public static final String MODE_REPLAY = "replay";

    private final EventSource eventSource;
    private final WindowManager windowManager;
    private final FeatureEngineConfig config;
    private final KafkaTemplate<String, FeatureComputedEvent> featureProducer;
    private final CheckpointManager checkpointManager;
    private final String mode;
    private final UnaryOperator<String> topicResolver;

    private final Map<Integer, Long> currentOffsets = new HashMap<>();
    private Instant lastCheckpointWatermark = Instant.MIN;

    // Metrics
    private final Counter eventsProcessed;
    private final Counter outputsEmitted;
    private final Timer featureLatency;
    private final AtomicLong watermarkLagMs = new AtomicLong(0);
    private final AtomicReference<Instant> lastWatermark = new AtomicReference<>(Instant.MIN);

    private volatile boolean running = false;

    /**
     * Live-mode constructor. The runner publishes to the topic returned by
     * {@link FeatureComputedEvent#topicName()} unchanged.
     */
    public FeatureEngineRunner(
            EventSource eventSource,
            WindowManager windowManager,
            FeatureEngineConfig config,
            KafkaTemplate<String, FeatureComputedEvent> featureProducer,
            CheckpointManager checkpointManager,
            MeterRegistry meterRegistry
    ) {
        this(eventSource, windowManager, config, featureProducer, checkpointManager, meterRegistry,
                MODE_LIVE, UnaryOperator.identity());
    }

    /**
     * Mode-aware constructor. The {@code mode} tag is added to every metric so that
     * live and replay metrics are independently observable. The {@code topicResolver}
     * transforms output topic names — typically identity for live and
     * {@code t -> t + ".replay"} for replay runs.
     *
     * <p>See {@code DETERMINISTIC_REPLAY.md} §How Live and Replay Share One Path.</p>
     */
    public FeatureEngineRunner(
            EventSource eventSource,
            WindowManager windowManager,
            FeatureEngineConfig config,
            KafkaTemplate<String, FeatureComputedEvent> featureProducer,
            CheckpointManager checkpointManager,
            MeterRegistry meterRegistry,
            String mode,
            UnaryOperator<String> topicResolver
    ) {
        if (mode == null || mode.isBlank()) throw new IllegalArgumentException("mode is required");
        if (topicResolver == null) throw new IllegalArgumentException("topicResolver is required");
        this.eventSource = eventSource;
        this.windowManager = windowManager;
        this.config = config;
        this.featureProducer = featureProducer;
        this.checkpointManager = checkpointManager;
        this.mode = mode;
        this.topicResolver = topicResolver;

        // Register metrics per OBSERVABILITY_STRATEGY.md §Feature Engine.
        // The `mode` tag distinguishes live from replay runs so shadow-replay
        // observability remains clean.
        this.eventsProcessed = Counter.builder("muninn.feature.events.processed")
                .tag("feature", VwapComputer.FEATURE_NAME)
                .tag("version", VwapComputer.FEATURE_VERSION)
                .tag("mode", mode)
                .register(meterRegistry);

        this.outputsEmitted = Counter.builder("muninn.feature.outputs.emitted")
                .tag("feature", VwapComputer.FEATURE_NAME)
                .tag("version", VwapComputer.FEATURE_VERSION)
                .tag("mode", mode)
                .register(meterRegistry);

        this.featureLatency = Timer.builder("muninn.feature.latency")
                .tag("feature", VwapComputer.FEATURE_NAME)
                .tag("version", VwapComputer.FEATURE_VERSION)
                .tag("mode", mode)
                .register(meterRegistry);

        Gauge.builder("muninn.feature.watermark.lag", watermarkLagMs, AtomicLong::get)
                .tag("feature", VwapComputer.FEATURE_NAME)
                .tag("mode", mode)
                .register(meterRegistry);
    }

    public String mode() {
        return mode;
    }

    @Override
    public void run() {
        running = true;
        eventSource.start();

        log.atInfo()
                .addKeyValue("feature", VwapComputer.FEATURE_NAME)
                .addKeyValue("windowDuration", config.windowDuration())
                .addKeyValue("codeVersion", config.codeVersion())
                .log("Feature engine started");

        try {
            while (running && eventSource.hasMore()) {
                Optional<EventSource.PartitionedEvent> polled = eventSource.poll();

                if (polled.isEmpty()) {
                    continue;
                }

                EventSource.PartitionedEvent pe = polled.get();
                MarketEvent event = pe.event();
                currentOffsets.put(pe.partition(), pe.offset());

                // Only process TradeEvents for VWAP
                if (event instanceof TradeEvent trade) {
                    windowManager.add(trade, pe.partition());
                    eventsProcessed.increment();

                    // Update watermark lag gauge
                    Instant wm = windowManager.globalWatermark();
                    lastWatermark.set(wm);

                    // Check if we should checkpoint
                    if (shouldCheckpoint(wm)) {
                        doCheckpoint(wm);
                    }
                }

                // Try to fire completed windows after every event
                windowManager.fireCompletedWindows(batch -> {
                    Timer.Sample sample = Timer.start();

                    FeatureComputedEvent result = VwapComputer.compute(batch, config.codeVersion());

                    // Publish to Kafka. The topicResolver lets replay runners
                    // route to a sibling topic (e.g., features.vwap.1m.v1.replay)
                    // without changing the event itself.
                    String topic = topicResolver.apply(result.topicName());
                    featureProducer.send(topic, "BTC-USDT", result);

                    outputsEmitted.increment();
                    sample.stop(featureLatency);

                    log.atInfo()
                            .addKeyValue("feature", result.featureName())
                            .addKeyValue("windowStart", result.windowStart())
                            .addKeyValue("windowEnd", result.windowEnd())
                            .addKeyValue("vwap", result.value())
                            .addKeyValue("tradeCount", batch.size())
                            .log("Window closed — VWAP emitted");
                });
            }
        } catch (Exception e) {
            log.atError()
                    .addKeyValue("feature", VwapComputer.FEATURE_NAME)
                    .addKeyValue("error", e.getMessage())
                    .log("Feature engine error");
            throw e;
        } finally {
            eventSource.stop();
            running = false;
            log.atInfo().log("Feature engine stopped");
        }
    }

    /**
     * Signal the engine to stop gracefully.
     */
    public void stop() {
        running = false;
    }

    /**
     * @return true if the engine loop is running
     */
    public boolean isRunning() {
        return running;
    }

    private boolean shouldCheckpoint(Instant currentWatermark) {
        if (currentWatermark == null || currentWatermark.equals(Instant.MIN) || currentWatermark.equals(Instant.MAX)) {
            return false;
        }

        if (lastCheckpointWatermark.equals(Instant.MIN)) {
            lastCheckpointWatermark = currentWatermark;
            return true;
        }

        Duration elapsed = Duration.between(lastCheckpointWatermark, currentWatermark);
        return elapsed.compareTo(config.checkpointInterval()) >= 0;
    }

    private void doCheckpoint(Instant watermark) {
        CheckpointState state = new CheckpointState(
                VwapComputer.FEATURE_NAME,
                config.codeVersion(),
                watermark,
                windowManager.snapshot(),
                currentOffsets,
                Instant.now()
        );

        try {
            checkpointManager.write(state);
            lastCheckpointWatermark = watermark;
            log.atInfo()
                    .addKeyValue("watermark", watermark)
                    .log("Checkpoint saved");
        } catch (Exception e) {
            log.atError().setCause(e).log("Failed to save checkpoint");
            // Do not crash the engine; checkpointing is best-effort for recovery
        }
    }
}
