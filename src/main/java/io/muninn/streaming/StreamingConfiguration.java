package io.muninn.streaming;

import io.micrometer.core.instrument.MeterRegistry;
import io.muninn.shared.event.FeatureComputedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Wires the live feature-streaming (SSE) components: the in-memory {@link FeatureStreamBroker}, the
 * Kafka tail {@link FeatureStreamConsumer}, and the {@link SmartLifecycle} that runs the consumer on
 * its own thread.
 *
 * <p>The {@link FeatureStreamBroker} and {@link FeatureStreamController} always exist so the HTTP
 * endpoint is available; only the background tail thread is gated on
 * {@code muninn.streaming.enabled}. See {@code docs/adr/0009-streaming-features-sse.md}.</p>
 */
@Configuration
@EnableConfigurationProperties(StreamingProperties.class)
public class StreamingConfiguration {

    private static final Logger log = LoggerFactory.getLogger(StreamingConfiguration.class);

    @Bean
    public FeatureStreamBroker featureStreamBroker(StreamingProperties properties, MeterRegistry meterRegistry) {
        return new FeatureStreamBroker(properties, meterRegistry);
    }

    @Bean
    public FeatureStreamConsumer featureStreamConsumer(
            StreamingProperties properties,
            FeatureStreamBroker broker,
            MeterRegistry meterRegistry,
            KafkaConnectionDetails kafkaConnectionDetails) {

        // A FeatureComputedEvent-typed JSON deserializer that ignores the producer's type headers,
        // so the tail consumer always materializes the computed-event record regardless of the
        // __TypeId__ the feature engine stamped on the record.
        JsonDeserializer<FeatureComputedEvent> valueDeserializer = new JsonDeserializer<>(FeatureComputedEvent.class);
        valueDeserializer.addTrustedPackages("io.muninn.shared.event");
        valueDeserializer.ignoreTypeHeaders();

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnectionDetails.getConsumer().getBootstrapServers());
        // Unique group per process: every instance gets every partition (a broadcast tail), and a
        // restart never replays history or interferes with other consumer groups.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "muninn-streaming-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, FeatureComputedEvent> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);

        return new FeatureStreamConsumer(
                consumer, Pattern.compile(properties.topicPattern()), broker, properties.pollTimeout(), meterRegistry);
    }

    @Bean
    public SmartLifecycle featureStreamLifecycle(FeatureStreamConsumer consumer, StreamingProperties properties) {
        return new SmartLifecycle() {
            private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "feature-stream-consumer");
                thread.setDaemon(true);
                return thread;
            });
            private volatile boolean running = false;

            @Override
            public void start() {
                if (!properties.enabled()) {
                    log.atInfo().log("Feature streaming is disabled by configuration");
                    return;
                }
                log.atInfo().log("Starting feature stream tail consumer...");
                executor.submit(consumer);
                running = true;
            }

            @Override
            public void stop() {
                if (running) {
                    consumer.stop();
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                    running = false;
                }
            }

            @Override
            public boolean isRunning() {
                return running;
            }

            @Override
            public int getPhase() {
                return 100;
            }
        };
    }
}
