package io.muninn.feature.engine;

import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.feature.checkpoint.CheckpointState;
import io.muninn.feature.compute.VwapComputer;
import io.muninn.feature.microstructure.MicroPriceComputer;
import io.muninn.feature.microstructure.OrderBookImbalanceComputer;
import io.muninn.feature.microstructure.VPINComputer;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Instrument;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * The main feature engine loop.
 *
 * <p>Consumes events from an {@link EventSource}, routes them through the
 * {@link WindowManager}, and — when a window closes — dispatches the resulting
 * {@link WindowedBatch} to every registered {@link WindowFeatureComputer}
 * ({@code VwapComputer}, {@code OrderBookImbalanceComputer}, {@code MicroPriceComputer})
 * plus {@code VPINComputer} (handled separately; see below), publishing each
 * {@link FeatureComputedEvent} produced to Kafka.</p>
 *
 * <p>This class is the orchestrator — it does not contain feature computation logic.
 * Computation is delegated to pure functions. Three of the four registered computers
 * are memoryless functions of "this window's batch" and fit the uniform
 * {@link WindowFeatureComputer} shape. VPIN is dispatched separately because its volume
 * buckets do not align to time windows: it needs an explicit {@code BucketState} threaded
 * across window calls. That state is owned here, keyed per {@link Instrument}, and is
 * never held as mutable state on the computer itself — see {@code VPINComputer}'s Javadoc
 * and DETERMINISTIC_REPLAY.md §Anti-Patterns.</p>
 *
 * <p>The engine runs in a single thread within the feature-engine loop. It is
 * started and stopped by Spring lifecycle management.</p>
 */
public final class FeatureEngineRunner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FeatureEngineRunner.class);

    public static final String MODE_LIVE = "live";
    public static final String MODE_REPLAY = "replay";

    /**
     * Key used for the shared, engine-level checkpoint (the buffered-window state that
     * every registered feature reads from) — distinct from any single feature's name,
     * since the window buffer is no longer VWAP-specific once book snapshots also flow
     * through it. See DETERMINISTIC_REPLAY.md §Checkpoints.
     */
    static final String ENGINE_CHECKPOINT_NAME = "feature-engine.shared-window-state";

    /**
     * Single-instrument MVP: every publish uses this Kafka message key. Generalizing to
     * multi-instrument routing is tracked separately (see ARCHITECTURE_REVIEW.md
     * "Single-instrument hardcoding") and is not part of this change.
     */
    private static final String INSTRUMENT_KEY = "BTC-USDT";

    private final EventSource eventSource;
    private final WindowManager windowManager;
    private final FeatureEngineConfig config;
    private final KafkaTemplate<String, FeatureComputedEvent> featureProducer;
    private final CheckpointManager checkpointManager;
    private final MeterRegistry meterRegistry;
    private final String mode;
    private final UnaryOperator<String> topicResolver;

    /** Computers that are pure functions of "this window's batch" alone. */
    private final List<WindowFeatureComputer> windowComputers;

    /** VPIN's volume-bucket state, keyed per instrument — see class Javadoc. */
    private final Map<Instrument, VPINComputer.BucketState> vpinState = new ConcurrentHashMap<>();

    private final Map<Integer, Long> currentOffsets = new HashMap<>();
    private Instant lastCheckpointWatermark = Instant.MIN;

    // Metrics
    private final Timer featureLatency;
    private final Counter unknownSideVolumeCounter;
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
        this.meterRegistry = meterRegistry;
        this.mode = mode;
        this.topicResolver = topicResolver;

        // Registered, uniformly-dispatched computers (deliverable: "dispatch over
        // registered FeatureComputers instead of hardcoding VwapComputer.compute").
        // VwapComputer.compute() throws on a window with no trades rather than
        // returning Optional, so it is wrapped here to fit the shared shape; OBI and
        // MicroPrice already return Optional natively.
        this.windowComputers = List.of(
                (batch, codeVersion) -> batch.trades().isEmpty()
                        ? Optional.empty()
                        : Optional.of(VwapComputer.compute(batch, codeVersion)),
                (batch, codeVersion) -> OrderBookImbalanceComputer.compute(batch, config.obiLevels(), codeVersion),
                MicroPriceComputer::compute
        );

        // Register metrics per OBSERVABILITY_STRATEGY.md §Feature Engine. The `mode`
        // tag distinguishes live from replay runs so shadow-replay observability
        // remains clean. `feature` and `version` tags are now applied dynamically at
        // publish time (via meterRegistry.counter/.timer) rather than fixed to VWAP at
        // construction time, since the engine dispatches more than one feature.
        Gauge.builder("muninn.feature.watermark.lag", watermarkLagMs, AtomicLong::get)
                .tag("mode", mode)
                .register(meterRegistry);

        this.featureLatency = Timer.builder("muninn.feature.latency")
                .tag("mode", mode)
                .register(meterRegistry);

        this.unknownSideVolumeCounter = Counter.builder("muninn.feature.vpin.unknown.side.volume")
                .tag("feature", VPINComputer.FEATURE_NAME)
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

        // Deterministic-recovery: seed engine state + watermark from the latest
        // checkpoint BEFORE consuming, so a restart does not silently drop
        // partially-accumulated windows or rewind the watermark (sre-data-ops-5).
        restoreFromCheckpoint();

        log.atInfo()
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

                // Admit every market event — trades, book snapshots, whatever else
                // arrives — into the shared window buffer. The engine does not decide
                // here which feature(s) an event is "for"; that is each computer's job
                // once the window closes (see class Javadoc).
                windowManager.add(event, pe.partition());
                meterRegistry.counter("muninn.feature.events.processed",
                        "topic", event.topicName(), "mode", mode).increment();

                // Update watermark lag gauge
                Instant wm = windowManager.globalWatermark();
                lastWatermark.set(wm);
                if (wm != null && !wm.equals(Instant.MIN) && !wm.equals(Instant.MAX)) {
                    watermarkLagMs.set(Instant.now().toEpochMilli() - wm.toEpochMilli());
                }

                // Check if we should checkpoint
                if (shouldCheckpoint(wm)) {
                    doCheckpoint(wm);
                }

                // Try to fire completed windows after every event
                windowManager.fireCompletedWindows(this::dispatch);
            }
        } catch (Exception e) {
            log.atError()
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
     * Dispatch a closed window to every registered computer and publish whatever each
     * one produces. This is the generic replacement for the old hardcoded
     * {@code VwapComputer.compute(batch, codeVersion)} call.
     */
    private void dispatch(WindowedBatch batch) {
        for (WindowFeatureComputer computer : windowComputers) {
            Timer.Sample sample = Timer.start();
            computer.compute(batch, config.codeVersion()).ifPresent(result -> {
                publish(result);
                sample.stop(featureLatency);
            });
        }
        dispatchVpin(batch);
    }

    /**
     * VPIN is dispatched separately from {@link #windowComputers} because it is not a
     * pure function of the batch alone — it needs its running volume-bucket state,
     * threaded explicitly and keyed per instrument so a multi-instrument engine cannot
     * cross-contaminate buckets (see {@code VPINComputer}'s Javadoc).
     */
    private void dispatchVpin(WindowedBatch batch) {
        List<TradeEvent> trades = batch.trades();
        if (trades.isEmpty()) {
            return; // nothing for VPIN this window; state is unchanged, no lookup needed
        }

        Instrument instrument = trades.get(0).instrument();
        VPINComputer.BucketState state = vpinState.getOrDefault(instrument, VPINComputer.BucketState.EMPTY);

        Timer.Sample sample = Timer.start();
        VPINComputer.Result result = VPINComputer.compute(
                state, batch, config.vpinBucketVolumeSize(), config.vpinNumberOfBuckets(), config.codeVersion());

        vpinState.put(instrument, result.nextState());

        if (result.unknownSideVolumeProcessed().signum() > 0) {
            unknownSideVolumeCounter.increment(result.unknownSideVolumeProcessed().doubleValue());
            log.atWarn()
                    .addKeyValue("instrument", instrument.symbol())
                    .addKeyValue("unknownSideVolume", result.unknownSideVolumeProcessed())
                    .log("VPIN processed trades with an unknown side — 50/50 split applied");
        }

        result.event().ifPresent(event -> {
            publish(event);
            sample.stop(featureLatency);
        });
    }

    /**
     * Publish a computed feature event to its (mode-resolved) output topic and record
     * the associated per-feature metrics.
     */
    private void publish(FeatureComputedEvent result) {
        String topic = topicResolver.apply(result.topicName());
        featureProducer.send(topic, INSTRUMENT_KEY, result);

        meterRegistry.counter("muninn.feature.outputs.emitted",
                "feature", result.featureName(), "version", result.featureVersion(), "mode", mode).increment();

        log.atInfo()
                .addKeyValue("feature", result.featureName())
                .addKeyValue("version", result.featureVersion())
                .addKeyValue("windowStart", result.windowStart())
                .addKeyValue("windowEnd", result.windowEnd())
                .addKeyValue("value", result.value())
                .addKeyValue("inputEventCount", result.inputEventIds().size())
                .log("Window closed — feature emitted");
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

    /**
     * Seed engine state from the most recent persisted checkpoint, if one exists.
     *
     * <p>Restores, in order: buffered (incomplete) window state, the per-partition
     * watermark, the consumer offsets seen so far, and the checkpoint-cadence cursor.
     * If no checkpoint exists the engine starts cold. Restore is best-effort and
     * <em>fails open</em>: any error is logged and the engine continues from a cold
     * start rather than refusing to boot — never leave the engine unable to start.</p>
     *
     * <p>VPIN's per-instrument bucket state is intentionally NOT part of this
     * checkpoint — only the shared window buffer is checkpointed today. A restart
     * therefore resets VPIN's moving average to a cold start; this is a known,
     * scoped-out limitation (see the PR description), not an oversight.</p>
     */
    private void restoreFromCheckpoint() {
        try {
            Optional<CheckpointState> latest =
                    checkpointManager.restoreLatest(ENGINE_CHECKPOINT_NAME, config.codeVersion());

            if (latest.isEmpty()) {
                log.atInfo()
                        .addKeyValue("version", config.codeVersion())
                        .log("No checkpoint to restore — starting cold");
                return;
            }

            CheckpointState state = latest.get();

            // 1. Re-seed the watermark first so restored windows are evaluated against
            //    the correct watermark and not prematurely fired or dropped as late.
            //    The global watermark is the min across partitions; seeding every known
            //    partition to that value reproduces the captured global watermark.
            Instant watermark = state.watermark();
            for (Integer partition : state.consumerOffsets().keySet()) {
                windowManager.seedWatermark(partition, watermark);
            }

            // 2. Restore buffered window state.
            int restoredWindows = windowManager.restore(state.windowStates());

            // 3. Restore consumer offsets and checkpoint cadence cursor.
            currentOffsets.putAll(state.consumerOffsets());
            lastWatermark.set(watermark);
            lastCheckpointWatermark = watermark;

            log.atInfo()
                    .addKeyValue("feature", state.featureName())
                    .addKeyValue("version", state.featureVersion())
                    .addKeyValue("watermark", watermark)
                    .addKeyValue("restoredWindows", restoredWindows)
                    .addKeyValue("partitions", state.consumerOffsets().size())
                    .log("Feature engine state restored from checkpoint");

        } catch (Exception e) {
            // Fail open: a restore failure must not prevent the engine from starting.
            log.atError()
                    .setCause(e)
                    .log("Checkpoint restore failed — starting cold");
        }
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
                ENGINE_CHECKPOINT_NAME,
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
