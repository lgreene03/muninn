package io.muninn.replay;

import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

/**
 * Creates ephemeral Kafka consumers for replay jobs.
 *
 * <p>Each replay job gets a unique consumer group to avoid interfering with
 * live consumers or other replay jobs.</p>
 *
 * <p>Bootstrap address is sourced from {@link KafkaConnectionDetails} so
 * test-time {@code @ServiceConnection} overrides apply.</p>
 */
@Component
public class ReplayConsumerFactory {

    private final String bootstrapServers;

    public ReplayConsumerFactory(KafkaConnectionDetails kafkaConnectionDetails) {
        this.bootstrapServers = String.join(",", kafkaConnectionDetails.getConsumerBootstrapServers());
    }

    /**
     * Create a new Kafka consumer for a replay job.
     *
     * @param request the replay request
     * @return a new consumer with a unique group ID
     */
    public KafkaConsumer<String, MarketEvent> create(ReplayRequest request) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "muninn-replay-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "io.muninn.shared.event");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MarketEvent.class.getName());
        return new KafkaConsumer<>(props);
    }
}
