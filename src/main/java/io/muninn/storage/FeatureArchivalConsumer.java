package io.muninn.storage;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.storage.sink.FeatureSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes computed features from Kafka and flushes them to the archival
 * {@link FeatureSink} — Parquet locally, Iceberg in production-reference.
 *
 * <p>Batching is intentionally simple (fixed-size flush) for MVP. A
 * time-based flush could be added once smoke-test throughput indicates it
 * would matter; until then the bound is the manual 10-event flush below.</p>
 */
@Service
public class FeatureArchivalConsumer {

    private static final Logger log = LoggerFactory.getLogger(FeatureArchivalConsumer.class);
    private static final int BATCH_SIZE = 10;

    private final FeatureSink sink;
    private final List<FeatureComputedEvent> buffer = new ArrayList<>();
    private final AtomicInteger processedCount = new AtomicInteger(0);

    public FeatureArchivalConsumer(FeatureSink sink) {
        this.sink = sink;
        log.atInfo()
                .addKeyValue("sink", sink.sinkId())
                .log("Feature archival consumer ready");
    }

    @KafkaListener(topics = "features.vwap.v1", groupId = "muninn-archival",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(FeatureComputedEvent event) {
        synchronized (buffer) {
            buffer.add(event);
            processedCount.incrementAndGet();

            if (buffer.size() >= BATCH_SIZE) {
                flush();
            }
        }
    }

    private void flush() {
        if (buffer.isEmpty()) return;

        try {
            FeatureSink.SinkWriteResult result = sink.write("BTC-USDT", buffer);
            log.atInfo()
                    .addKeyValue("sink", result.sinkId())
                    .addKeyValue("location", result.location())
                    .addKeyValue("eventsFlushed", result.rowCount())
                    .log("Flushed feature events");
            buffer.clear();
        } catch (Exception e) {
            log.atError().setCause(e).log("Failed to flush feature events");
            // MVP: log and clear. A real deployment would route to a dead-letter
            // topic or retry with backoff. See RUNBOOK.md §Storage.
            buffer.clear();
        }
    }
}
