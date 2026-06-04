package io.muninn.streaming;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.shared.event.FeatureComputedEvent;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FeatureStreamBrokerTest {

    private static final StreamingProperties PROPS = new StreamingProperties(
            true, "features\\..*", Duration.ofMillis(500), Duration.ZERO, Duration.ofSeconds(15));

    private static FeatureComputedEvent event(String featureName) {
        return new FeatureComputedEvent(
                UUID.randomUUID(),
                Instant.parse("2026-06-03T00:00:00Z"),
                featureName,
                "v1",
                new BigDecimal("100.0"),
                null,
                Instant.parse("2026-06-03T00:00:00Z"),
                Instant.parse("2026-06-03T00:01:00Z"),
                List.of(UUID.randomUUID()),
                "abc123");
    }

    @Test
    void broadcast_unfilteredSubscriber_receivesEvent() throws IOException {
        FeatureStreamBroker broker = new FeatureStreamBroker(PROPS, new SimpleMeterRegistry());
        SseEmitter emitter = mock(SseEmitter.class);
        broker.registerEmitter(emitter, null);
        assertThat(broker.activeCount()).isEqualTo(1);

        broker.broadcast(event("vwap.1m"));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void broadcast_filteredSubscriber_receivesOnlyMatchingFeature() throws IOException {
        FeatureStreamBroker broker = new FeatureStreamBroker(PROPS, new SimpleMeterRegistry());
        SseEmitter emitter = mock(SseEmitter.class);
        broker.registerEmitter(emitter, "vwap.1m");

        broker.broadcast(event("obi.1m"));
        verify(emitter, never()).send(any(SseEmitter.SseEventBuilder.class));

        broker.broadcast(event("vwap.1m"));
        verify(emitter, times(1)).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void broadcast_sendFailure_prunesSubscriber() throws IOException {
        FeatureStreamBroker broker = new FeatureStreamBroker(PROPS, new SimpleMeterRegistry());
        SseEmitter emitter = mock(SseEmitter.class);
        doThrow(new IOException("client gone")).when(emitter).send(any(SseEmitter.SseEventBuilder.class));
        broker.registerEmitter(emitter, null);
        assertThat(broker.activeCount()).isEqualTo(1);

        broker.broadcast(event("vwap.1m"));

        assertThat(broker.activeCount()).isZero();
    }

    @Test
    void registerEmitter_tracksActiveGauge() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        FeatureStreamBroker broker = new FeatureStreamBroker(PROPS, registry);

        broker.registerEmitter(mock(SseEmitter.class), null);
        broker.registerEmitter(mock(SseEmitter.class), null);

        assertThat(registry.get("muninn.streaming.subscriptions.active").gauge().value()).isEqualTo(2.0);
    }
}
