package io.muninn.streaming;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.muninn.shared.event.FeatureComputedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.regex.Pattern;

/**
 * Tails the feature output topics ({@code features.*}) and hands each {@link FeatureComputedEvent}
 * to the {@link FeatureStreamBroker} for fan-out to connected SSE clients.
 *
 * <p>Runs on a single dedicated thread (managed by a {@code SmartLifecycle} in
 * {@link StreamingConfiguration}). The underlying {@link Consumer} uses a unique group id and
 * starts from the latest offset, so the stream is a live tail — it never replays history and it
 * never commits offsets. {@link #stop()} is safe to call from another thread.</p>
 */
public class FeatureStreamConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FeatureStreamConsumer.class);

    private final Consumer<String, FeatureComputedEvent> consumer;
    private final Pattern topicPattern;
    private final FeatureStreamBroker broker;
    private final Duration pollTimeout;
    private final Counter eventsReceived;

    private volatile boolean running = true;

    public FeatureStreamConsumer(
            Consumer<String, FeatureComputedEvent> consumer,
            Pattern topicPattern,
            FeatureStreamBroker broker,
            Duration pollTimeout,
            MeterRegistry meterRegistry) {
        this.consumer = consumer;
        this.topicPattern = topicPattern;
        this.broker = broker;
        this.pollTimeout = pollTimeout;
        this.eventsReceived = Counter.builder("muninn.streaming.events.received")
                .tag("endpoint", "features")
                .description("Feature events read from Kafka by the stream tail consumer")
                .register(meterRegistry);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topicPattern);
            log.atInfo().addKeyValue("pattern", topicPattern.pattern()).log("Feature stream tail consumer started");
            while (running) {
                try {
                    drain(consumer.poll(pollTimeout));
                } catch (WakeupException e) {
                    if (running) {
                        throw e;
                    }
                    // expected during shutdown
                }
            }
        } finally {
            consumer.close();
            log.atInfo().log("Feature stream tail consumer stopped");
        }
    }

    // Package-private so tests can exercise record handling without a live Kafka consumer.
    void drain(Iterable<ConsumerRecord<String, FeatureComputedEvent>> records) {
        for (ConsumerRecord<String, FeatureComputedEvent> record : records) {
            FeatureComputedEvent event = record.value();
            if (event == null) {
                continue;
            }
            eventsReceived.increment();
            broker.broadcast(event);
        }
    }

    /** Signal the tail loop to stop and unblock any in-progress {@code poll()}. Thread-safe. */
    public void stop() {
        running = false;
        consumer.wakeup();
    }
}
