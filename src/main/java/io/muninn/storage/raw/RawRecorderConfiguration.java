package io.muninn.storage.raw;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.Map;

/**
 * Wires the raw-event recorder. Off unless {@code muninn.recorder.enabled=true}.
 *
 * <p>The consumer reads values as raw bytes, never through the typed JSON deserialiser,
 * so what is stored is exactly what the broker held. {@code auto.offset.reset=earliest}
 * means the first start also records whatever Kafka still retains, recovering up to a
 * day of trades that would otherwise expire unrecorded.
 *
 * <p>Depends on the shared {@link S3Client} from {@code StorageConfig.newS3Client}. Two
 * defects in that client would each have made this recorder useless while it appeared to
 * run, both fixed in muninn#64: it ignored {@code muninn.storage.s3.endpoint} and always
 * pointed at {@code localhost:9002}, which inside a container is muninn itself; and the
 * AWS SDK's default {@code aws-chunked} checksum encoding made SeaweedFS store the chunk
 * framing as the object, so every file would have been unreadable.
 */
@Configuration
@ConditionalOnProperty(prefix = "muninn.recorder", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(RawRecorderProperties.class)
public class RawRecorderConfiguration {

    @Bean
    public RawRecorderLifecycle rawRecorderLifecycle(RawRecorderProperties props,
                                                     KafkaConnectionDetails kafka,
                                                     S3Client s3,
                                                     MeterRegistry meters) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getConsumer().getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // Offsets move only when the recorder commits, which it does after storing.
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);

        RawEventRecorder recorder = new RawEventRecorder(
                new KafkaConsumer<>(cfg),
                new RawBatchWriter(new S3ObjectPutter(s3, props.bucket())),
                props, meters, System::nanoTime);
        return new RawRecorderLifecycle(recorder);
    }

    /** Owns the recorder's dedicated thread. */
    public static final class RawRecorderLifecycle implements SmartLifecycle {

        private static final Logger log = LoggerFactory.getLogger(RawRecorderLifecycle.class);

        private final RawEventRecorder recorder;
        private volatile boolean running;
        private Thread thread;

        RawRecorderLifecycle(RawEventRecorder recorder) {
            this.recorder = recorder;
        }

        @Override
        public synchronized void start() {
            if (running) {
                return;
            }
            running = true;
            thread = new Thread(() -> {
                try {
                    recorder.run(() -> running);
                } catch (RuntimeException e) {
                    // Loud on purpose: a dead recorder is history being lost.
                    log.atError().setCause(e).log("Raw event recorder stopped unexpectedly");
                }
            }, "muninn-raw-recorder");
            thread.setDaemon(false);
            thread.start();
        }

        @Override
        public synchronized void stop() {
            if (!running) {
                return;
            }
            running = false;
            recorder.wakeup();
            try {
                thread.join(30_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        public RawEventRecorder recorder() {
            return recorder;
        }
    }
}
