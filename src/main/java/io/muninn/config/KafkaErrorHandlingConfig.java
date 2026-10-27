package io.muninn.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Poison-message handling for the {@code @KafkaListener} consumers (sre-data-ops-3).
 *
 * <p>Without an error handler a single record that cannot be processed (or cannot be
 * deserialized) is retried forever by the container and blocks its partition — no
 * subsequent record on that partition is ever consumed. This config makes the consumer
 * fail-fast on poison records and move on:</p>
 *
 * <ul>
 *   <li>Value deserialization is wrapped in an {@code ErrorHandlingDeserializer} (configured
 *       in {@code application.yml}) delegating to the JSON deserializer, so a bad payload
 *       surfaces as a {@code DeserializationException} instead of killing the poll loop.</li>
 *   <li>The {@link DefaultErrorHandler} below retries a record a small, bounded number of
 *       times ({@link FixedBackOff}) and then hands it to a
 *       {@link DeadLetterPublishingRecoverer} which republishes it to {@code <topic>.DLT}.</li>
 *   <li>Each record sent to the DLT increments a {@code muninn.kafka.poison.messages}
 *       counter so poison volume is observable.</li>
 * </ul>
 *
 * <p>The error handler is exposed as a {@link DefaultErrorHandler} bean; Spring Boot's
 * Kafka auto-configuration wires a single such bean into the auto-configured
 * {@code ConcurrentKafkaListenerContainerFactory} automatically.</p>
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /** Suffix appended to the source topic to form its dead-letter topic. */
    static final String DLT_SUFFIX = ".DLT";

    /** Number of retries before a record is sent to the DLT (total attempts = retries + 1). */
    static final long MAX_RETRIES = 2L;

    /** Fixed delay between retries, in milliseconds. */
    static final long RETRY_INTERVAL_MS = 500L;

    /**
     * Producer factory for dead-lettering a record whose VALUE failed to deserialize: the
     * recoverer republishes the original raw value bytes. The key is a {@link String}
     * because keys are read with {@code StringDeserializer}, which cannot fail, so the key
     * of every such record has already been deserialized.
     *
     * <p>Both used to be {@link ByteArraySerializer}. The key then failed to serialize
     * ("Can't convert key of class java.lang.String"), so publishing to the DLT threw,
     * and the error handler re-seeked the same record for ever: 6,947 attempts a minute
     * on one offset, the very partition block this class exists to prevent. Its test
     * mocked the template, so no serializer ever ran.</p>
     */
    @Bean
    public ProducerFactory<String, byte[]> deadLetterByteProducerFactory(
            KafkaConnectionDetails kafkaConnectionDetails) {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getProducer().getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    @Bean
    public KafkaTemplate<String, byte[]> deadLetterByteTemplate(
            ProducerFactory<String, byte[]> deadLetterByteProducerFactory) {
        return new KafkaTemplate<>(deadLetterByteProducerFactory);
    }

    /**
     * Recoverer that publishes unrecoverable records to {@code <topic>.DLT} on the same
     * partition number, and increments the poison-message counter.
     *
     * <p>The template is chosen by the record's value, in map order. A record that failed
     * to deserialize has a null value and goes to the first entry, the raw-bytes
     * template. A record that deserialized but failed processing still holds its object,
     * which goes to the JSON template ({@code KafkaConfig#deadLetterTemplate}), the same
     * serializer every muninn producer uses.</p>
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, byte[]> deadLetterByteTemplate,
            KafkaTemplate<String, Object> deadLetterTemplate,
            MeterRegistry meterRegistry) {

        Counter poisonCounter = Counter.builder("muninn.kafka.poison.messages")
                .description("Records routed to a dead-letter topic after exhausting retries")
                .register(meterRegistry);

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, deadLetterByteTemplate);
        templates.put(Object.class, deadLetterTemplate);

        return new DeadLetterPublishingRecoverer(
                templates,
                (record, exception) -> {
                    poisonCounter.increment();
                    String dltTopic = record.topic() + DLT_SUFFIX;
                    log.atWarn()
                            .addKeyValue("topic", record.topic())
                            .addKeyValue("partition", record.partition())
                            .addKeyValue("offset", record.offset())
                            .addKeyValue("dlt", dltTopic)
                            .addKeyValue("cause", exception.getMessage())
                            .log("Routing poison record to dead-letter topic");
                    return new TopicPartition(dltTopic, record.partition());
                });
    }

    /**
     * Error handler used by the auto-configured listener container factory. Retries a
     * record {@link #MAX_RETRIES} times with a {@link #RETRY_INTERVAL_MS}ms fixed backoff,
     * then routes it to the DLT via the recoverer.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer,
                                                 MeterRegistry meterRegistry) {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                neverBlocking(recoverer, meterRegistry), new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
        // A deserialization failure is deterministic — never worth retrying; send it
        // straight to the DLT on the first encounter.
        handler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class);
        return handler;
    }

    /**
     * Wraps the recoverer so a record that cannot be dead-lettered is skipped, not retried.
     *
     * <p>When a recoverer throws, {@link DefaultErrorHandler} re-seeks the record and
     * tries again, for ever if the failure is deterministic, as a serialization failure
     * is. Blocking the partition is the outcome this class exists to prevent, so a failed
     * dead-letter publish is logged with its cause and the record's coordinates, counted
     * in {@code muninn.kafka.poison.unpublished}, and the consumer moves on. Nothing is
     * lost by skipping: the record is still in its source topic at the logged offset.</p>
     */
    static ConsumerRecordRecoverer neverBlocking(ConsumerRecordRecoverer recoverer, MeterRegistry meterRegistry) {
        Counter unpublished = Counter.builder("muninn.kafka.poison.unpublished")
                .description("Poison records skipped because they could not be dead-lettered")
                .register(meterRegistry);
        return (record, exception) -> {
            try {
                recoverer.accept(record, exception);
            } catch (RuntimeException publishFailure) {
                unpublished.increment();
                log.atError()
                        .setCause(publishFailure)
                        .addKeyValue("topic", record.topic())
                        .addKeyValue("partition", record.partition())
                        .addKeyValue("offset", record.offset())
                        .log("Poison record could not be dead-lettered; skipping it so the partition keeps moving");
            }
        };
    }
}
