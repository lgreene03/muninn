package io.muninn.feature.engine;

import io.micrometer.core.instrument.MeterRegistry;
import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties(FeatureEngineConfig.class)
public class FeatureEngineConfiguration {

    private static final Logger log = LoggerFactory.getLogger(FeatureEngineConfiguration.class);

    @Bean
    public CheckpointManager checkpointManager(S3Client s3Client, MeterRegistry meterRegistry) {
        return new CheckpointManager(s3Client, meterRegistry);
    }

    @Bean
    public FeatureEngineRunner featureEngineRunner(
            FeatureEngineConfig config,
            KafkaTemplate<String, Object> kafkaTemplate,
            CheckpointManager checkpointManager,
            MeterRegistry meterRegistry,
            org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties,
            org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails kafkaConnectionDetails
    ) {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, FeatureComputedEvent> featureProducer = (KafkaTemplate<String, FeatureComputedEvent>) (Object) kafkaTemplate;
        // Create an isolated Kafka consumer for the live event source.
        // Override the bootstrap address with KafkaConnectionDetails (Spring Boot 3.1+)
        // so @ServiceConnection in tests overrides the static
        // spring.kafka.bootstrap-servers value bound into KafkaProperties.
        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties(null);
        consumerProps.put("bootstrap.servers", kafkaConnectionDetails.getConsumerBootstrapServers());
        consumerProps.put("group.id", "muninn-feature-engine");
        KafkaConsumer<String, MarketEvent> consumer = new KafkaConsumer<>(consumerProps);

        LiveEventSource eventSource = new LiveEventSource(consumer, List.of("events.trade"));
        
        WatermarkTracker watermarkTracker = new WatermarkTracker();
        WindowManager windowManager = new WindowManager(config.windowDuration(), watermarkTracker);

        return new FeatureEngineRunner(
                eventSource,
                windowManager,
                config,
                featureProducer,
                checkpointManager,
                meterRegistry
        );
    }

    @Bean
    public SmartLifecycle featureEngineLifecycle(FeatureEngineRunner runner, FeatureEngineConfig config) {
        return new SmartLifecycle() {
            private final ExecutorService executor = Executors.newSingleThreadExecutor();
            private volatile boolean isRunning = false;

            @Override
            public void start() {
                if (!config.enabled()) {
                    log.atInfo().log("Feature Engine is disabled by configuration");
                    return;
                }
                log.atInfo().log("Starting Feature Engine runner...");
                executor.submit(runner);
                isRunning = true;
            }

            @Override
            public void stop() {
                if (isRunning) {
                    log.atInfo().log("Stopping Feature Engine runner...");
                    runner.stop();
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                    isRunning = false;
                }
            }

            @Override
            public boolean isRunning() {
                return isRunning;
            }

            @Override
            public int getPhase() {
                // Start after basic services
                return 100;
            }
        };
    }
}
