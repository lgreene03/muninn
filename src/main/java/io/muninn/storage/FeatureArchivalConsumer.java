package io.muninn.storage;

import io.muninn.shared.event.FeatureComputedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes computed features from Kafka and flushes them to Parquet in MinIO.
 *
 * <p>Uses a simple batching strategy for the MVP: flushes every N events.</p>
 */
@Service
public class FeatureArchivalConsumer {

    private static final Logger log = LoggerFactory.getLogger(FeatureArchivalConsumer.class);
    private static final int BATCH_SIZE = 10; // Small batch size for MVP/Smoke testing

    private final FeatureParquetWriter parquetWriter;
    private final List<FeatureComputedEvent> buffer = new ArrayList<>();
    private final AtomicInteger processedCount = new AtomicInteger(0);

    public FeatureArchivalConsumer(FeatureParquetWriter parquetWriter) {
        this.parquetWriter = parquetWriter;
    }

    @KafkaListener(topics = "features.vwap.v1", groupId = "muninn-archival",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(FeatureComputedEvent event) {
        synchronized (buffer) {
            buffer.add(event);
            int count = processedCount.incrementAndGet();

            if (buffer.size() >= BATCH_SIZE) {
                flush();
            }
        }
    }

    private void flush() {
        if (buffer.isEmpty()) return;

        try {
            // Write to Parquet/MinIO
            String key = parquetWriter.write("BTC-USDT", buffer);
            log.atInfo()
                    .addKeyValue("eventsFlushed", buffer.size())
                    .addKeyValue("s3Key", key)
                    .log("Flushed feature events to Parquet");

            buffer.clear();
        } catch (Exception e) {
            log.atError().setCause(e).log("Failed to flush feature events to Parquet");
            // In a real app we'd trigger a dead-letter queue or retry; for MVP we log and clear.
            buffer.clear();
        }
    }
}
