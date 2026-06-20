package io.muninn.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link KafkaErrorHandlingConfig} (sre-data-ops-3).
 *
 * <p>Verifies that a poison (undeserializable) record does not block the partition: the
 * {@link DefaultErrorHandler} routes it to the {@code <topic>.DLT} dead-letter topic, the
 * poison-message counter is incremented, and the handler reports the record as recovered
 * so the container can advance past it.</p>
 */
class KafkaErrorHandlingConfigTest {

    private final KafkaErrorHandlingConfig config = new KafkaErrorHandlingConfig();

    private MeterRegistry meterRegistry;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<byte[], byte[]> dltTemplate = mock(KafkaTemplate.class);

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        // The recoverer publishes to the DLT via this template; return a completed future.
        when(dltTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    @Test
    void poisonRecord_routedToDlt_counterIncremented_andRecovered() {
        DeadLetterPublishingRecoverer recoverer =
                config.deadLetterPublishingRecoverer(dltTemplate, meterRegistry);
        DefaultErrorHandler handler = config.kafkaErrorHandler(recoverer);

        ConsumerRecord<String, Object> poison = new ConsumerRecord<>(
                "events.trade", 0, 42L, -1L, TimestampType.NO_TIMESTAMP_TYPE,
                -1, -1, "BTC-USDT", null, new RecordHeaders(), java.util.Optional.empty());

        // A deserialization failure is configured as non-retryable, so the first
        // encounter recovers (true) rather than retrying — the partition advances.
        Consumer<?, ?> consumer = mock(Consumer.class);
        MessageListenerContainer container = mock(MessageListenerContainer.class);

        Exception thrown = new DeserializationException(
                "bad json", new byte[]{1, 2, 3}, false, new RuntimeException("boom"));

        boolean recovered = handler.handleOne(thrown, poison, consumer, container);

        assertThat(recovered).as("poison record should be recovered, not blocking the partition").isTrue();

        // Published to the dead-letter topic.
        @SuppressWarnings("unchecked")
        var captor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(dltTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("events.trade" + KafkaErrorHandlingConfig.DLT_SUFFIX);

        // Poison counter incremented.
        assertThat(meterRegistry.get("muninn.kafka.poison.messages").counter().count()).isEqualTo(1.0);
    }
}
