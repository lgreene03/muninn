package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Replay event source — reads historical events from Kafka using seek-by-timestamp.
 *
 * <p>Implements the same {@link EventSource} interface as {@link LiveEventSource}.
 * The feature engine cannot tell the difference between live and replay sources —
 * if it needs to know, that is a design failure (DETERMINISTIC_REPLAY.md).</p>
 *
 * <p>After construction, call {@link #start()} to subscribe and seek to the
 * requested time range. The source terminates when all events in the range
 * have been consumed.</p>
 */
public final class ReplayEventSource implements EventSource {

    private static final Logger log = LoggerFactory.getLogger(ReplayEventSource.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final int MAX_EMPTY_POLLS = 50; // ~5 seconds of empty polls = done

    private final KafkaConsumer<String, MarketEvent> consumer;
    private final List<String> topics;
    private final Instant fromTime;
    private final Instant toTime;
    private final Queue<PartitionedEvent> buffer = new ArrayDeque<>();

    private volatile boolean running = false;
    private int emptyPollCount = 0;

    /**
     * @param consumer the Kafka consumer (must not be subscribed yet)
     * @param topics   the topics to replay from
     * @param fromTime inclusive start of the replay range
     * @param toTime   exclusive end of the replay range
     */
    public ReplayEventSource(
            KafkaConsumer<String, MarketEvent> consumer,
            List<String> topics,
            Instant fromTime,
            Instant toTime
    ) {
        this.consumer = consumer;
        this.topics = List.copyOf(topics);
        this.fromTime = fromTime;
        this.toTime = toTime;
    }

    @Override
    public void start() {
        // Subscribe and get assigned partitions
        consumer.subscribe(topics);
        // Force partition assignment
        consumer.poll(Duration.ofMillis(0));

        // Seek all partitions to the requested start time
        Set<TopicPartition> partitions = consumer.assignment();
        Map<TopicPartition, Long> timestampQuery = new HashMap<>();
        for (TopicPartition tp : partitions) {
            timestampQuery.put(tp, fromTime.toEpochMilli());
        }

        var offsets = consumer.offsetsForTimes(timestampQuery);
        for (var entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue().offset());
            } else {
                // No data at this timestamp — seek to beginning
                consumer.seekToBeginning(List.of(entry.getKey()));
            }
        }

        running = true;
        log.atInfo()
                .addKeyValue("topics", topics)
                .addKeyValue("fromTime", fromTime)
                .addKeyValue("toTime", toTime)
                .log("ReplayEventSource started — seeking to time range");
    }

    @Override
    public void stop() {
        running = false;
        consumer.close();
        log.atInfo().log("ReplayEventSource stopped");
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

        if (records.isEmpty()) {
            emptyPollCount++;
            if (emptyPollCount >= MAX_EMPTY_POLLS) {
                // No more data in the requested range
                running = false;
                log.atInfo().log("ReplayEventSource exhausted — no more events in range");
            }
            return Optional.empty();
        }

        emptyPollCount = 0;

        for (ConsumerRecord<String, MarketEvent> record : records) {
            MarketEvent event = record.value();
            Instant eventTime = event.eventTime();

            // Filter to the requested time range
            if (eventTime.isBefore(fromTime)) {
                continue; // before range — skip
            }
            if (!eventTime.isBefore(toTime)) {
                // Past the end of the range — we're done
                running = false;
                break;
            }

            buffer.add(new PartitionedEvent(event, record.partition(), record.offset()));
        }

        return buffer.isEmpty() ? Optional.empty() : Optional.of(buffer.poll());
    }

    @Override
    public boolean hasMore() {
        return running || !buffer.isEmpty();
    }
}
