package io.muninn.storage;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.time.UUIDv7;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Parquet write + read-back test.
 * Validates that FeatureComputedEvents round-trip through Parquet correctly.
 */
class FeatureParquetWriterTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void writeAndRead_singleEvent_roundTrips() throws IOException {
        FeatureComputedEvent event = buildEvent(
                Instant.parse("2026-05-11T14:00:00Z"),
                Instant.parse("2026-05-11T14:01:00Z"),
                "67511.38888889"
        );

        java.nio.file.Path parquetFile = tempDir.resolve("test.parquet");
        FeatureParquetWriter.writeParquetFile(parquetFile, List.of(event));

        List<GenericRecord> records = readParquet(parquetFile);
        assertThat(records).hasSize(1);

        GenericRecord record = records.getFirst();
        assertThat(record.get("event_id").toString()).isEqualTo(event.eventId().toString());
        assertThat(record.get("vwap_value").toString()).isEqualTo("67511.38888889");
        assertThat(record.get("feature_name").toString()).isEqualTo("vwap.1m");
        assertThat((long) record.get("window_start_ms")).isEqualTo(event.windowStart().toEpochMilli());
        assertThat((long) record.get("window_end_ms")).isEqualTo(event.windowEnd().toEpochMilli());
    }

    @Test
    void writeAndRead_multipleEvents_allPreserved() throws IOException {
        List<FeatureComputedEvent> events = List.of(
                buildEvent(Instant.parse("2026-05-11T14:00:00Z"), Instant.parse("2026-05-11T14:01:00Z"), "67511.38"),
                buildEvent(Instant.parse("2026-05-11T14:01:00Z"), Instant.parse("2026-05-11T14:02:00Z"), "67529.34"),
                buildEvent(Instant.parse("2026-05-11T14:02:00Z"), Instant.parse("2026-05-11T14:03:00Z"), "67544.50")
        );

        java.nio.file.Path parquetFile = tempDir.resolve("test-multi.parquet");
        FeatureParquetWriter.writeParquetFile(parquetFile, events);

        List<GenericRecord> records = readParquet(parquetFile);
        assertThat(records).hasSize(3);
        assertThat(records.get(0).get("vwap_value").toString()).isEqualTo("67511.38");
        assertThat(records.get(1).get("vwap_value").toString()).isEqualTo("67529.34");
        assertThat(records.get(2).get("vwap_value").toString()).isEqualTo("67544.50");
    }

    @Test
    void writeAndRead_preservesPrecision() throws IOException {
        FeatureComputedEvent event = buildEvent(
                Instant.parse("2026-05-11T14:00:00Z"),
                Instant.parse("2026-05-11T14:01:00Z"),
                "67523.12345678"
        );

        java.nio.file.Path parquetFile = tempDir.resolve("test-precision.parquet");
        FeatureParquetWriter.writeParquetFile(parquetFile, List.of(event));

        List<GenericRecord> records = readParquet(parquetFile);
        assertThat(records.getFirst().get("vwap_value").toString()).isEqualTo("67523.12345678");
    }

    private List<GenericRecord> readParquet(java.nio.file.Path path) throws IOException {
        List<GenericRecord> records = new ArrayList<>();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                        new LocalInputFile(path))
                .withConf(new PlainParquetConfiguration())
                .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                records.add(record);
            }
        }
        return records;
    }

    private FeatureComputedEvent buildEvent(Instant windowStart, Instant windowEnd, String vwap) {
        return new FeatureComputedEvent(
                UUIDv7.generate(),
                windowEnd,
                "vwap.1m",
                "v1",
                new BigDecimal(vwap),
                Map.of("tradeCount", BigDecimal.valueOf(42)),
                windowStart,
                windowEnd,
                List.of(UUID.randomUUID(), UUID.randomUUID()),
                "test-sha"
        );
    }
}
