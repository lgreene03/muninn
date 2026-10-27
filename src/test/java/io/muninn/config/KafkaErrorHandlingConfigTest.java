package io.muninn.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
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
 *
 * <p>The first test mocks the template, so no serializer runs. That is how a key
 * serializer that could not serialize any muninn key passed for months; the tests after
 * it publish through real serializers.</p>
 */
class KafkaErrorHandlingConfigTest {

    /** A record as obi-bridge writes it: JSON with no {@code __TypeId__} header. */
    private static final byte[] FOREIGN_JSON =
            "{\"eventTime\":\"2026-10-27T00:54:59Z\",\"featureName\":\"obi\",\"value\":0.12}"
                    .getBytes(StandardCharsets.UTF_8);

    private final KafkaErrorHandlingConfig config = new KafkaErrorHandlingConfig();

    private MeterRegistry meterRegistry;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, byte[]> dltTemplate = mock(KafkaTemplate.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> jsonTemplate = mock(KafkaTemplate.class);

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
                config.deadLetterPublishingRecoverer(dltTemplate, jsonTemplate, meterRegistry);
        DefaultErrorHandler handler = config.kafkaErrorHandler(recoverer, meterRegistry);

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

    // ── Through real serializers ────────────────────────────────────────────────

    /** The record exactly as muninn's consumer sees one it cannot deserialize. */
    private static ConsumerRecord<String, Object> undeserializableRecord() {
        RecordHeaders headers = new RecordHeaders();
        // The production delegate: JsonDeserializer with type mappings but no default
        // type, so a record without a type header fails. ErrorHandlingDeserializer turns
        // that into a null value plus an exception header, as in the running consumer.
        try (ErrorHandlingDeserializer<Object> deserializer =
                     new ErrorHandlingDeserializer<>(new JsonDeserializer<>())) {
            Object value = deserializer.deserialize("features.obi.v1", headers, FOREIGN_JSON);
            assertThat(value).as("precondition: the foreign record must fail to deserialize").isNull();
        }
        return new ConsumerRecord<>("features.obi.v1", 0, 256_528L, -1L, TimestampType.NO_TIMESTAMP_TYPE,
                -1, FOREIGN_JSON.length, "BTC-USDT", null, headers, Optional.empty());
    }

    /** Instantiates the serializer class the production factory is configured with. */
    @SuppressWarnings("unchecked")
    private <T> Serializer<T> configuredSerializer(String property) throws Exception {
        KafkaConnectionDetails details = mock(KafkaConnectionDetails.class, RETURNS_DEEP_STUBS);
        when(details.getProducer().getBootstrapServers()).thenReturn(List.of("localhost:9092"));
        Map<String, Object> props = config.deadLetterByteProducerFactory(details).getConfigurationProperties();
        return ((Class<? extends Serializer<T>>) props.get(property)).getDeclaredConstructor().newInstance();
    }

    private static <K, V> KafkaTemplate<K, V> templateOver(MockProducer<K, V> producer) {
        return new KafkaTemplate<>(new MockProducerFactory<>(() -> producer));
    }

    @Test
    void undeserializableRecordWithAStringKeyIsDeadLetteredThroughTheConfiguredSerializers() throws Exception {
        // The production failure: the key arrives as a String and the byte template's
        // key serializer could not serialize it, so every dead-letter publish threw.
        MockProducer<String, byte[]> bytes = new MockProducer<>(true, null,
                configuredSerializer(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),
                configuredSerializer(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        MockProducer<String, Object> json = new MockProducer<>(true, null, new StringSerializer(), new JsonSerializer<>());
        DefaultErrorHandler handler = config.kafkaErrorHandler(
                config.deadLetterPublishingRecoverer(templateOver(bytes), templateOver(json), meterRegistry),
                meterRegistry);

        boolean recovered = handler.handleOne(
                new DeserializationException("no type information", FOREIGN_JSON, false, new IllegalStateException()),
                undeserializableRecord(), mock(Consumer.class), mock(MessageListenerContainer.class));

        assertThat(recovered).isTrue();
        assertThat(bytes.history()).singleElement().satisfies(sent -> {
            assertThat(sent.topic()).isEqualTo("features.obi.v1.DLT");
            assertThat(sent.key()).isEqualTo("BTC-USDT");
            assertThat(sent.value()).as("the original bytes, unchanged").isEqualTo(FOREIGN_JSON);
        });
        assertThat(json.history()).isEmpty();
        assertThat(meterRegistry.get("muninn.kafka.poison.unpublished").counter().count()).isZero();
    }

    @Test
    void recordThatDeserializedButFailedProcessingIsDeadLetteredAsJson() {
        // Its value is an object, not bytes; a bytes-only template could never send it.
        MockProducer<String, byte[]> bytes = new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
        MockProducer<String, Object> json = new MockProducer<>(true, null, new StringSerializer(), new JsonSerializer<>());
        DeadLetterPublishingRecoverer recoverer =
                config.deadLetterPublishingRecoverer(templateOver(bytes), templateOver(json), meterRegistry);

        ConsumerRecord<String, Object> processed = new ConsumerRecord<>("features.vwap.1m.v1", 0, 7L,
                "BTC-USDT", Map.of("featureName", "vwap.1m"));
        recoverer.accept(processed, new IllegalStateException("downstream failure"));

        assertThat(json.history()).singleElement()
                .satisfies(sent -> assertThat(sent.topic()).isEqualTo("features.vwap.1m.v1.DLT"));
        assertThat(bytes.history()).isEmpty();
    }

    @Test
    void aDeadLetterPublishThatFailsSkipsTheRecordInsteadOfBlockingThePartition() {
        // Simulate the old configuration exactly: a byte[] key serializer meets a String
        // key. Before, the handler reported "not recovered" and re-seeked for ever.
        @SuppressWarnings({"unchecked", "rawtypes"})
        MockProducer<String, byte[]> brokenKeys =
                new MockProducer(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        MockProducer<String, Object> json = new MockProducer<>(true, null, new StringSerializer(), new JsonSerializer<>());
        DefaultErrorHandler handler = config.kafkaErrorHandler(
                config.deadLetterPublishingRecoverer(templateOver(brokenKeys), templateOver(json), meterRegistry),
                meterRegistry);

        boolean recovered = handler.handleOne(
                new DeserializationException("no type information", FOREIGN_JSON, false, new IllegalStateException()),
                undeserializableRecord(), mock(Consumer.class), mock(MessageListenerContainer.class));

        assertThat(recovered).as("the partition must move on even when the DLT cannot be written").isTrue();
        assertThat(meterRegistry.get("muninn.kafka.poison.unpublished").counter().count()).isEqualTo(1.0);
    }
}
