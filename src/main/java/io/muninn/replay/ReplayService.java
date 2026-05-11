package io.muninn.replay;

import io.muninn.shared.event.MarketEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Replay service — re-reads events from the event log within a specified time range.
 *
 * <p>This is the Phase 0/1 replay skeleton. Phase 4 will extend this with checkpoint
 * support, divergence detection, and feature-engine integration.</p>
 */
@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final ReplayConsumerFactory consumerFactory;

    public ReplayService(ReplayConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    /**
     * Replay events from the specified topic within the time range.
     *
     * @param request the replay request specifying topic and time range
     * @param handler callback invoked for each replayed event
     * @return the result containing event count and elapsed time
     */
    public ReplayResult replay(ReplayRequest request, Consumer<MarketEvent> handler) {
        log.atInfo()
                .addKeyValue("topic", request.topic())
                .addKeyValue("from", request.from())
                .addKeyValue("to", request.to())
                .log("Starting replay");

        long eventsReplayed = 0;
        Instant start = Instant.now();

        try (KafkaConsumer<String, MarketEvent> consumer = consumerFactory.create(request)) {
            var partitions = consumer.partitionsFor(request.topic()).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();

            consumer.assign(partitions);

            Map<TopicPartition, Long> startOffsets = consumer.offsetsForTimes(
                    partitions.stream().collect(Collectors.toMap(
                            tp -> tp, tp -> request.from().toEpochMilli()
                    ))
            ).entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            startOffsets.forEach(consumer::seek);

            long toMs = request.to().toEpochMilli();
            boolean done = false;

            while (!done) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, MarketEvent> record : records) {
                    if (record.timestamp() > toMs) {
                        done = true;
                        break;
                    }
                    handler.accept(record.value());
                    eventsReplayed++;
                }
            }
        }

        Duration elapsed = Duration.between(start, Instant.now());
        log.atInfo()
                .addKeyValue("eventsReplayed", eventsReplayed)
                .addKeyValue("elapsedMs", elapsed.toMillis())
                .log("Replay complete");

        return new ReplayResult(eventsReplayed, elapsed);
    }
}
