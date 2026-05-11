package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.TradeEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Live event source backed by a Kafka consumer subscribed to trade topics.
 *
 * <p>Polls from Redpanda/Kafka, deserializes {@link TradeEvent}s, and exposes
 * them through the {@link EventSource} interface. Events are buffered
 * internally to amortize poll overhead.</p>
 */
public final class LiveEventSource implements EventSource {

    private static final Logger log = LoggerFactory.getLogger(LiveEventSource.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

    private final KafkaConsumer<String, MarketEvent> consumer;
    private final List<String> topics;
    private final Queue<PartitionedEvent> buffer = new ArrayDeque<>();
    private volatile boolean running = false;

    public LiveEventSource(KafkaConsumer<String, MarketEvent> consumer, List<String> topics) {
        this.consumer = consumer;
        this.topics = List.copyOf(topics);
    }

    @Override
    public void start() {
        consumer.subscribe(topics);
        running = true;
        log.atInfo()
                .addKeyValue("topics", topics)
                .log("LiveEventSource started");
    }

    @Override
    public void stop() {
        running = false;
        consumer.close();
        log.atInfo().log("LiveEventSource stopped");
    }

    @Override
    public Optional<PartitionedEvent> poll() {
        if (!buffer.isEmpty()) {
            return Optional.of(buffer.poll());
        }

        if (!running) {
            return Optional.empty();
        }

        ConsumerRecords<String, MarketEvent> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, MarketEvent> record : records) {
            buffer.add(new PartitionedEvent(record.value(), record.partition(), record.offset()));
        }

        return buffer.isEmpty() ? Optional.empty() : Optional.of(buffer.poll());
    }

    @Override
    public boolean hasMore() {
        return running || !buffer.isEmpty();
    }
}
