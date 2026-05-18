package io.muninn.config;

import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Kafka producer configuration for the typed application producers
 * ({@code marketEventKafkaTemplate}, {@code deadLetterTemplate}).
 *
 * <p>The bootstrap address is sourced from Spring Boot's
 * {@link KafkaConnectionDetails} so it respects {@code @ServiceConnection}
 * overrides in tests and any other {@code ConnectionDetails} source in
 * production. The previous implementation kept its own
 * {@code KafkaProperties} record with a hardcoded default, which silently
 * bypassed property binding.</p>
 *
 * <p>Producers are idempotent with {@code acks=all} per
 * {@code DATA_STORAGE_STRATEGY.md}.</p>
 */
@Configuration
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, MarketEvent> marketEventProducerFactory(
            KafkaConnectionDetails kafkaConnectionDetails) {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getProducerBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
        ));
    }

    @Bean
    public KafkaTemplate<String, MarketEvent> marketEventKafkaTemplate(
            ProducerFactory<String, MarketEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, Object> deadLetterProducerFactory(
            KafkaConnectionDetails kafkaConnectionDetails) {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getProducerBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all"
        ));
    }

    @Bean
    public KafkaTemplate<String, Object> deadLetterTemplate(
            ProducerFactory<String, Object> deadLetterProducerFactory) {
        return new KafkaTemplate<>(deadLetterProducerFactory);
    }
}
