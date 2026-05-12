package io.muninn.replay;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.feature.compute.VwapComputer;
import io.muninn.feature.engine.FeatureEngineConfig;
import io.muninn.feature.engine.FeatureEngineRunner;
import io.muninn.feature.engine.ReplayEventSource;
import io.muninn.feature.engine.WatermarkTracker;
import io.muninn.feature.engine.WindowManager;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Executes {@link ReplayJob}s by running a fresh feature-engine instance
 * over a {@link ReplayEventSource} and routing outputs to a {@code .replay}
 * sibling topic.
 *
 * <p>The same {@link FeatureEngineRunner} class drives the replay — this is the
 * "one computation path" guarantee from {@code DETERMINISTIC_REPLAY.md}. The
 * differences are only in the source ({@code ReplayEventSource} vs
 * {@code LiveEventSource}), the metric mode tag ({@code "replay"}), and the
 * output topic suffix.</p>
 *
 * <p>Jobs run on a single shared worker thread in MVP. Concurrent replays are a
 * future optimization once cross-job determinism is well-tested.</p>
 */
@Service
public class ReplayJobRunner {

    private static final Logger log = LoggerFactory.getLogger(ReplayJobRunner.class);

    /** Suffix appended to live output topic names so replay outputs land on a sibling topic. */
    public static final String REPLAY_TOPIC_SUFFIX = ".replay";

    private final FeatureEngineConfig config;
    private final KafkaTemplate<String, FeatureComputedEvent> featureProducer;
    private final CheckpointManager checkpointManager;
    private final MeterRegistry meterRegistry;
    private final KafkaProperties kafkaProperties;
    private final ReplayJobRegistry registry;

    private final ExecutorService worker;
    private final ConcurrentMap<UUID, FeatureEngineRunner> activeRunners = new ConcurrentHashMap<>();

    public ReplayJobRunner(
            FeatureEngineConfig config,
            KafkaTemplate<String, Object> kafkaTemplate,
            CheckpointManager checkpointManager,
            MeterRegistry meterRegistry,
            KafkaProperties kafkaProperties,
            ReplayJobRegistry registry
    ) {
        this.config = config;
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, FeatureComputedEvent> typed =
                (KafkaTemplate<String, FeatureComputedEvent>) (Object) kafkaTemplate;
        this.featureProducer = typed;
        this.checkpointManager = checkpointManager;
        this.meterRegistry = meterRegistry;
        this.kafkaProperties = kafkaProperties;
        this.registry = registry;

        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "muninn-replay-worker");
            t.setDaemon(true);
            return t;
        };
        this.worker = Executors.newSingleThreadExecutor(tf);
    }

    /**
     * Submit a new replay job. Returns immediately with the job's initial
     * {@code PENDING} state; the worker picks it up asynchronously.
     */
    public ReplayJob submit(ReplayJob job) {
        registry.put(job);
        worker.submit(() -> runJob(job));
        log.atInfo()
                .addKeyValue("jobId", job.jobId())
                .addKeyValue("from", job.from())
                .addKeyValue("to", job.to())
                .addKeyValue("topics", job.topics())
                .log("Replay job submitted");
        return job;
    }

    /**
     * Run a job synchronously on the calling thread. Useful for tests.
     */
    public ReplayJob runJob(ReplayJob initial) {
        Instant started = Instant.now();
        ReplayJob job = initial.started(started);
        registry.put(job);

        FeatureEngineRunner runner = null;
        try {
            runner = buildRunner(job);
            activeRunners.put(job.jobId(), runner);

            Timer.Sample sample = Timer.start();
            runner.run();
            sample.stop(Timer.builder("muninn.replay.job.duration")
                    .tag("feature", VwapComputer.FEATURE_NAME)
                    .tag("status", "completed")
                    .register(meterRegistry));

            Instant finished = Instant.now();
            ReplayJob completed = job.completed(
                    finished,
                    job.eventsReplayed(),
                    Duration.between(started, finished));
            registry.put(completed);

            log.atInfo()
                    .addKeyValue("jobId", job.jobId())
                    .addKeyValue("elapsedMs", completed.elapsed().toMillis())
                    .log("Replay job completed");
            return completed;
        } catch (Exception e) {
            Instant finished = Instant.now();
            ReplayJob failed = job.failed(finished, e.getMessage(),
                    Duration.between(started, finished));
            registry.put(failed);
            log.atError()
                    .addKeyValue("jobId", job.jobId())
                    .setCause(e)
                    .log("Replay job failed");
            return failed;
        } finally {
            activeRunners.remove(job.jobId());
        }
    }

    private FeatureEngineRunner buildRunner(ReplayJob job) {
        Map<String, Object> consumerProps = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        // Each replay gets its own consumer group so it can independently
        // seek-by-timestamp without interfering with the live consumer.
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "muninn-replay-" + job.jobId());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "io.muninn.shared.event");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                "io.muninn.shared.event.TradeEvent");

        KafkaConsumer<String, MarketEvent> consumer = new KafkaConsumer<>(consumerProps);
        ReplayEventSource source = new ReplayEventSource(
                consumer, job.topics(), job.from(), job.to());

        // Fresh per-job state — no shared windowing or watermarks with the live engine.
        WatermarkTracker watermarkTracker = new WatermarkTracker();
        WindowManager windowManager = new WindowManager(config.windowDuration(), watermarkTracker);

        return new FeatureEngineRunner(
                source,
                windowManager,
                config,
                featureProducer,
                checkpointManager,
                meterRegistry,
                FeatureEngineRunner.MODE_REPLAY,
                topic -> topic + REPLAY_TOPIC_SUFFIX);
    }

    public void shutdown() {
        worker.shutdown();
    }
}
