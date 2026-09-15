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
import java.util.ArrayList;
import java.util.Comparator;
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

        // A single poll() can return records from more than one subscribed topic
        // (e.g. events.trade and events.book.snapshot). ConsumerRecords groups them
        // per TopicPartition in Kafka-client-internal fetch order, which has no
        // relationship to event time across DIFFERENT topics — only within a single
        // partition is offset order guaranteed to be event-time order. Sort this
        // batch by event time before buffering so cross-topic events are delivered
        // to WindowManager in event-time order as closely as this batch allows,
        // per DETERMINISTIC_REPLAY.md §Event Ordering Assumptions ("the engine
        // merges multiple partitions using a k-way merge by event time"). This does
        // not guarantee global ordering across separate poll() calls, only within
        // one — see ReplayEventSource's identical treatment for the same reason.
        List<PartitionedEvent> batch = new ArrayList<>();
        for (ConsumerRecord<String, MarketEvent> record : records) {
            batch.add(new PartitionedEvent(record.value(), record.partition(), record.offset()));
        }
        batch.sort(Comparator.comparing(pe -> pe.event().eventTime()));
        buffer.addAll(batch);

        return buffer.isEmpty() ? Optional.empty() : Optional.of(buffer.poll());
    }

    @Override
    public boolean hasMore() {
        return running || !buffer.isEmpty();
    }
}
