package io.muninn.replay;

import io.muninn.event.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final ReplayConsumerFactory consumerFactory;

    public ReplayService(ReplayConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public ReplayResult replay(ReplayRequest request, Consumer<Event> handler) {
        log.info("Starting replay: topic={}, from={}, to={}", request.topic(), request.from(), request.to());
        long eventsReplayed = 0;
        Instant start = Instant.now();

        try (KafkaConsumer<String, Event> consumer = consumerFactory.create(request)) {
            List<TopicPartition> partitions = consumer.partitionsFor(request.topic()).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();

            consumer.assign(partitions);

            Map<TopicPartition, Long> startOffsets = consumer.offsetsForTimes(
                    partitions.stream().collect(java.util.stream.Collectors.toMap(
                            tp -> tp, tp -> request.from().toEpochMilli()
                    ))
            ).entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

            startOffsets.forEach(consumer::seek);

            long toMs = request.to().toEpochMilli();
            boolean done = false;

            while (!done) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) break;

                for (ConsumerRecord<String, Event> record : records) {
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
        log.info("Replay complete: {} events in {}ms", eventsReplayed, elapsed.toMillis());
        return new ReplayResult(eventsReplayed, elapsed);
    }
}
