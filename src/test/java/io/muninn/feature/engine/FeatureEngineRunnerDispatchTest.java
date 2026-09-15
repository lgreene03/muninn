package io.muninn.feature.engine;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Exercises {@link FeatureEngineRunner}'s generalized dispatch loop end-to-end
 * (in-process, no Kafka broker): a window containing both a book snapshot and a
 * trade must produce VWAP, OBI, micro-price AND VPIN outputs, proving the runner no
 * longer hardcodes {@code VwapComputer.compute} and admits {@link MarketEvent}s of
 * more than one concrete type (deliverables 1 and 2 of the P2 roadmap).
 *
 * <p>Uses a scripted in-memory {@link EventSource} rather than a real Kafka consumer
 * so this runs as a fast unit test; the Kafka-wiring-level proof lives in
 * {@code ReplayDeterminismIntegrationTest}-style Testcontainers coverage.</p>
 */
class FeatureEngineRunnerDispatchTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    @SuppressWarnings("unchecked")
    @Test
    void run_windowWithSnapshotAndTrade_dispatchesAllFourRegisteredFeatures() {
        FeatureEngineConfig config = new FeatureEngineConfig(
                true,
                Duration.ofMinutes(1),
                Duration.ofMinutes(5),
                "dispatch-test",
                "drop",
                5,                       // obiLevels
                new BigDecimal("0.50"), // vpinBucketVolumeSize — exactly the trade size below
                1                        // vpinNumberOfBuckets — one bucket is enough to be "ready"
        );

        Instant t0 = Instant.parse("2026-05-11T14:00:10Z");
        Instant t1 = Instant.parse("2026-05-11T14:00:20Z");
        Instant t2 = Instant.parse("2026-05-11T14:01:10Z"); // advances the watermark past window 1

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

        ScriptedEventSource source = new ScriptedEventSource(List.of(
                new EventSource.PartitionedEvent(snapshot, 0, 0L),
                new EventSource.PartitionedEvent(trade, 0, 1L),
                new EventSource.PartitionedEvent(closer, 0, 2L)
        ));

        WindowManager windowManager = new WindowManager(config.windowDuration(), new WatermarkTracker());
        KafkaTemplate<String, FeatureComputedEvent> kafkaTemplate = mock(KafkaTemplate.class);
        CheckpointManager checkpointManager = new CheckpointManager(mock(S3Client.class), new SimpleMeterRegistry());

        FeatureEngineRunner runner = new FeatureEngineRunner(
                source, windowManager, config, kafkaTemplate, checkpointManager, new SimpleMeterRegistry());

        runner.run();

        ArgumentCaptor<FeatureComputedEvent> captor = ArgumentCaptor.forClass(FeatureComputedEvent.class);
        verify(kafkaTemplate, atLeast(4)).send(anyString(), anyString(), captor.capture());

        List<String> emittedFeatures = captor.getAllValues().stream().map(FeatureComputedEvent::featureName).toList();

        assertThat(emittedFeatures)
                .as("all four registered computers must be dispatched from one closed window, not just VWAP")
                .contains("vwap.1m", "obi", "micro_price", "vpin");
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
