package io.muninn.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

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
     * Producer factory for the dead-letter publisher. Uses {@link ByteArraySerializer}
     * for both key and value because the recoverer republishes the original raw bytes of
     * a record that failed to deserialize.
     */
    @Bean
    public ProducerFactory<byte[], byte[]> deadLetterByteProducerFactory(
            KafkaConnectionDetails kafkaConnectionDetails) {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getProducer().getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    @Bean
    public KafkaTemplate<byte[], byte[]> deadLetterByteTemplate(
            ProducerFactory<byte[], byte[]> deadLetterByteProducerFactory) {
        return new KafkaTemplate<>(deadLetterByteProducerFactory);
    }

    /**
     * Recoverer that publishes unrecoverable records to {@code <topic>.DLT} on the same
     * partition number, and increments the poison-message counter.
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<byte[], byte[]> deadLetterByteTemplate,
            MeterRegistry meterRegistry) {

        Counter poisonCounter = Counter.builder("muninn.kafka.poison.messages")
                .description("Records routed to a dead-letter topic after exhausting retries")
                .register(meterRegistry);

        return new DeadLetterPublishingRecoverer(
                deadLetterByteTemplate,
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
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler handler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
        // A deserialization failure is deterministic — never worth retrying; send it
        // straight to the DLT on the first encounter.
        handler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class);
        return handler;
    }
}
