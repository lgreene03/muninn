package io.muninn.streaming;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.shared.event.FeatureComputedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class FeatureStreamConsumerTest {

    @SuppressWarnings("unchecked")
    private final Consumer<String, FeatureComputedEvent> kafkaConsumer = mock(Consumer.class);
    private final FeatureStreamBroker broker = mock(FeatureStreamBroker.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final FeatureStreamConsumer consumer = new FeatureStreamConsumer(
            kafkaConsumer, Pattern.compile("features\\..*"), broker, Duration.ofMillis(10), registry);

    private static FeatureComputedEvent event() {
        return new FeatureComputedEvent(
                UUID.randomUUID(), Instant.parse("2026-06-03T00:00:00Z"), "vwap.1m", "v1",
                new BigDecimal("1"), null,
                Instant.parse("2026-06-03T00:00:00Z"), Instant.parse("2026-06-03T00:01:00Z"),
                List.of(UUID.randomUUID()), "sha");
    }

    private static ConsumerRecord<String, FeatureComputedEvent> record(long offset, FeatureComputedEvent value) {
        return new ConsumerRecord<>("features.vwap.1m.v1", 0, offset, "BTC-USDT", value);
    }

    @Test
    void drain_forwardsEachEventAndCountsIt() {
        FeatureComputedEvent e1 = event();
        FeatureComputedEvent e2 = event();

        consumer.drain(List.of(record(0L, e1), record(1L, e2)));

        verify(broker).broadcast(e1);
        verify(broker).broadcast(e2);
        assertThat(registry.get("muninn.streaming.events.received").counter().count()).isEqualTo(2.0);
    }

    @Test
    void drain_skipsTombstoneValues() {
        consumer.drain(List.of(record(0L, null)));

        verify(broker, never()).broadcast(any());
        assertThat(registry.get("muninn.streaming.events.received").counter().count()).isZero();
    }
}
