package io.muninn.replay;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.feature.engine.EventSource;
import io.muninn.feature.engine.FeatureEngineConfig;
import io.muninn.feature.engine.FeatureEngineRunner;
import io.muninn.feature.engine.WatermarkTracker;
import io.muninn.feature.engine.WindowManager;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import io.muninn.storage.FeatureArchivalConsumer;
import io.muninn.streaming.StreamingProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.s3.S3Client;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * muninn's feature topics live under {@code muninn.features.}, never under a bare
 * {@code features.} (norse-stack decision D1, ADR-0016).
 *
 * <p>The bare namespace is not muninn's. In norse-stack, {@code features.obi.v1} belongs to
 * obi-bridge and is huginn's trading input, so a muninn feature landing there would trade,
 * and muninn's subscriptions reading there would choke on obi-bridge's records. This test
 * pins both directions: every producer path (live and replay, all four registered
 * computers) writes under the prefix, and every subscription (shadow comparator, live
 * stream, archival) reads under it and cannot match {@code features.obi.v1}.</p>
 *
 * <p>Topic names are written out literally on purpose. A test that built its expectations
 * from the production constant would still pass if that constant went back to
 * {@code "features."}.</p>
 */
class FeatureTopicNamespaceTest {

    private static final String MUNINN_PREFIX = "muninn.features.";
    private static final String BARE_PREFIX = "features.";
    private static final String OBI_BRIDGE_TOPIC = "features.obi.v1";

    private static final Set<String> LIVE_TOPICS = Set.of(
            "muninn.features.vwap.1m.v1",
            "muninn.features.obi.v1",
            "muninn.features.micro_price.v1",
            "muninn.features.vpin.v1");

    private static final Set<String> REPLAY_TOPICS = Set.of(
            "muninn.features.vwap.1m.v1.replay",
            "muninn.features.obi.v1.replay",
            "muninn.features.micro_price.v1.replay",
            "muninn.features.vpin.v1.replay");

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    // ---- Producers ---------------------------------------------------------------

    @Test
    void liveEngine_publishesEveryFeatureUnderTheMuninnPrefix() {
        KafkaTemplate<String, FeatureComputedEvent> producer = producer();
        FeatureEngineRunner live = new FeatureEngineRunner(
                script(), windowManager(), CONFIG, producer, checkpoints(), new SimpleMeterRegistry());

        live.run();

        assertThat(publishedTopics(producer))
                .as("the live engine's topics, one per registered computer")
                .containsExactlyInAnyOrderElementsOf(LIVE_TOPICS)
                .allSatisfy(t -> assertThat(t).startsWith(MUNINN_PREFIX).doesNotStartWith(BARE_PREFIX));
        verify(producer, never()).send(startsWith(BARE_PREFIX), anyString(), any(FeatureComputedEvent.class));
    }

    @Test
    void replayEngine_publishesEveryFeatureUnderTheMuninnPrefix() {
        KafkaTemplate<String, FeatureComputedEvent> producer = producer();
        FeatureEngineRunner replay = new FeatureEngineRunner(
                script(), windowManager(), CONFIG, producer, checkpoints(), new SimpleMeterRegistry(),
                FeatureEngineRunner.MODE_REPLAY, ReplayJobRunner::replayTopic);

        replay.run();

        assertThat(publishedTopics(producer))
                .as("the replay engine's topics: the .replay sibling of each live topic")
                .containsExactlyInAnyOrderElementsOf(REPLAY_TOPICS)
                .allSatisfy(t -> assertThat(t).startsWith(MUNINN_PREFIX).doesNotStartWith(BARE_PREFIX));
        verify(producer, never()).send(startsWith(BARE_PREFIX), anyString(), any(FeatureComputedEvent.class));
    }

    @Test
    void engine_refusesToPublishOutsideTheMuninnNamespace_whateverTheResolver() {
        // A resolver that strips the prefix would put muninn's OBI on obi-bridge's topic,
        // straight into huginn's trading input. The engine must refuse, not send.
        UnaryOperator<String> stripPrefix = t -> t.startsWith("muninn.") ? t.substring("muninn.".length()) : t;
        KafkaTemplate<String, FeatureComputedEvent> producer = producer();
        FeatureEngineRunner misrouted = new FeatureEngineRunner(
                script(), windowManager(), CONFIG, producer, checkpoints(), new SimpleMeterRegistry(),
                FeatureEngineRunner.MODE_LIVE, stripPrefix);

        assertThatThrownBy(misrouted::run)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(MUNINN_PREFIX);
        verify(producer, never()).send(startsWith(BARE_PREFIX), anyString(), any(FeatureComputedEvent.class));
    }

    @Test
    void replayJobOutputSink_reportedToTheCaller_isUnderTheMuninnPrefix() {
        ReplayJobController controller = new ReplayJobController(mock(ReplayJobRunner.class), new ReplayJobRegistry());

        ReplayJob job = controller.submit(new ReplayJobController.SubmitRequest(
                null, Instant.parse("2026-05-11T14:00:00Z"), Instant.parse("2026-05-11T14:02:00Z"), null)).getBody();

        assertThat(job).isNotNull();
        assertThat(job.outputSink()).isEqualTo("muninn.features.v1.replay");
    }

    // ---- Subscriptions -----------------------------------------------------------

    @Test
    void shadowComparatorLivePattern_matchesMuninnLiveTopicsOnly() {
        Pattern live = Pattern.compile(ShadowReplayComparator.LIVE_TOPIC_PATTERN);

        assertThat(LIVE_TOPICS).allSatisfy(t -> assertThat(live.matcher(t).matches()).as(t).isTrue());
        assertThat(live.matcher(OBI_BRIDGE_TOPIC).matches()).as(OBI_BRIDGE_TOPIC).isFalse();
        assertThat(live.matcher("features.vwap.1m.v1").matches()).as("bare vwap").isFalse();
        assertThat(REPLAY_TOPICS).allSatisfy(t -> assertThat(live.matcher(t).matches()).as(t).isFalse());
        assertThat(live.matcher("muninn.features.obi.v1.DLT").matches()).as("dead-letter topic").isFalse();
    }

    @Test
    void shadowComparatorReplayPattern_matchesMuninnReplayTopicsOnly() {
        Pattern replay = Pattern.compile(ShadowReplayComparator.REPLAY_TOPIC_PATTERN);

        assertThat(REPLAY_TOPICS).allSatisfy(t -> assertThat(replay.matcher(t).matches()).as(t).isTrue());
        assertThat(replay.matcher(OBI_BRIDGE_TOPIC).matches()).as(OBI_BRIDGE_TOPIC).isFalse();
        assertThat(replay.matcher("features.obi.v1.replay").matches()).as("bare replay").isFalse();
        assertThat(LIVE_TOPICS).allSatisfy(t -> assertThat(replay.matcher(t).matches()).as(t).isFalse());
    }

    @Test
    void shadowComparatorPatterns_matchWhatTheEnginesActuallyPublish() {
        // Ties the subscription to the producer rather than to a list kept by hand.
        KafkaTemplate<String, FeatureComputedEvent> liveProducer = producer();
        new FeatureEngineRunner(script(), windowManager(), CONFIG, liveProducer, checkpoints(),
                new SimpleMeterRegistry()).run();
        KafkaTemplate<String, FeatureComputedEvent> replayProducer = producer();
        new FeatureEngineRunner(script(), windowManager(), CONFIG, replayProducer, checkpoints(),
                new SimpleMeterRegistry(), FeatureEngineRunner.MODE_REPLAY, ReplayJobRunner::replayTopic).run();

        Pattern live = Pattern.compile(ShadowReplayComparator.LIVE_TOPIC_PATTERN);
        Pattern replay = Pattern.compile(ShadowReplayComparator.REPLAY_TOPIC_PATTERN);
        assertThat(publishedTopics(liveProducer)).allSatisfy(t -> assertThat(live.matcher(t).matches()).as(t).isTrue());
        assertThat(publishedTopics(replayProducer)).allSatisfy(t -> assertThat(replay.matcher(t).matches()).as(t).isTrue());
    }

    @Test
    void liveStreamDefaultPattern_matchesMuninnTopicsAndNotObiBridge() {
        Pattern stream = Pattern.compile(new StreamingProperties(true, null, null, null, null).topicPattern());

        assertThat(LIVE_TOPICS).allSatisfy(t -> assertThat(stream.matcher(t).matches()).as(t).isTrue());
        assertThat(stream.matcher(OBI_BRIDGE_TOPIC).matches()).as(OBI_BRIDGE_TOPIC).isFalse();
        assertThat(stream.matcher("features.vwap.1m.v1").matches()).as("bare vwap").isFalse();
    }

    @Test
    void archivalSubscription_isTheTopicVwapIsActuallyPublishedTo() throws NoSuchMethodException {
        KafkaListener listener = FeatureArchivalConsumer.class
                .getMethod("consume", FeatureComputedEvent.class)
                .getAnnotation(KafkaListener.class);

        assertThat(listener.topics())
                .as("archival must read the topic the live engine writes VWAP to, under the muninn prefix")
                .containsExactly("muninn.features.vwap.1m.v1");
    }

    // ---- Fixture -----------------------------------------------------------------

    private static final FeatureEngineConfig CONFIG = new FeatureEngineConfig(
            true,
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            "namespace-test",
            "drop",
            5,                       // obiLevels
            new BigDecimal("0.50"), // vpinBucketVolumeSize: exactly the trade size below
            1,                       // vpinNumberOfBuckets: one bucket is enough to be ready
            null,                    // instrumentExchange: default "binance", matching BTC_USDT
            null                     // instrumentSymbol: default "BTC-USDT"
    );

    /** One window holding a book snapshot and a trade, then a trade that closes it: all four computers fire. */
    private static EventSource script() {
        Instant t0 = Instant.parse("2026-05-11T14:00:10Z");
        Instant t1 = Instant.parse("2026-05-11T14:00:20Z");
        Instant t2 = Instant.parse("2026-05-11T14:01:10Z");

        OrderBookSnapshotEvent snapshot = new OrderBookSnapshotEvent(
                UUIDv7.generate(), t0, t0, "test", BTC_USDT, 1L, 1,
                List.of(new PriceLevel(new BigDecimal("100.00"), new BigDecimal("9.00"))),
                List.of(new PriceLevel(new BigDecimal("101.00"), new BigDecimal("1.00"))),
                1);
        TradeEvent trade = new TradeEvent(
                UUIDv7.generate(), t1, t1, "test", BTC_USDT, 1L, 1,
                new BigDecimal("100.50"), new BigDecimal("0.50"), Side.BUY, "t-1");
        TradeEvent closer = new TradeEvent(
                UUIDv7.generate(), t2, t2, "test", BTC_USDT, 2L, 1,
                new BigDecimal("100.50"), new BigDecimal("0.01"), Side.BUY, "t-2");

        return new ScriptedEventSource(List.of(
                new EventSource.PartitionedEvent(snapshot, 0, 0L),
                new EventSource.PartitionedEvent(trade, 0, 1L),
                new EventSource.PartitionedEvent(closer, 0, 2L)));
    }

    private static WindowManager windowManager() {
        return new WindowManager(CONFIG.windowDuration(), new WatermarkTracker());
    }

    private static CheckpointManager checkpoints() {
        return new CheckpointManager(mock(S3Client.class), new SimpleMeterRegistry());
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, FeatureComputedEvent> producer() {
        return mock(KafkaTemplate.class);
    }

    private static Set<String> publishedTopics(KafkaTemplate<String, FeatureComputedEvent> producer) {
        ArgumentCaptor<String> topics = ArgumentCaptor.forClass(String.class);
        verify(producer, atLeast(1)).send(topics.capture(), anyString(), any(FeatureComputedEvent.class));
        return topics.getAllValues().stream().collect(Collectors.toSet());
    }

    /** A minimal, in-memory {@link EventSource} that plays back a fixed script. */
    private static final class ScriptedEventSource implements EventSource {
        private final Deque<PartitionedEvent> queue;

        ScriptedEventSource(List<PartitionedEvent> events) {
            this.queue = new ArrayDeque<>(events);
        }

        @Override
        public Optional<PartitionedEvent> poll() {
            return Optional.ofNullable(queue.poll());
        }

        @Override
        public boolean hasMore() {
            return !queue.isEmpty();
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        public void stop() {
            // no-op
        }
    }
}
