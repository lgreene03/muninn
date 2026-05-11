package io.muninn.storage;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.exception.StorageException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Writes {@link FeatureComputedEvent}s to Hive-partitioned Parquet files.
 *
 * <p>Layout follows DATA_STORAGE_STRATEGY.md:</p>
 * <pre>
 *   muninn-warehouse/
 *     features.vwap.v1/
 *       instrument=BTC-USDT/
 *         year=2026/month=05/day=11/hour=14/
 *           part-00000-{uuid}.parquet
 * </pre>
 *
 * <p>Files are written locally first, then uploaded to MinIO. This approach
 * avoids partial writes and ensures atomicity.</p>
 */
public final class FeatureParquetWriter {

    private static final Logger log = LoggerFactory.getLogger(FeatureParquetWriter.class);

    /**
     * Avro schema for feature output Parquet files.
     */
    static final Schema FEATURE_SCHEMA = SchemaBuilder.record("FeatureOutput")
            .namespace("io.muninn.feature")
            .fields()
            .requiredString("event_id")
            .requiredLong("event_time_ms")
            .requiredLong("window_start_ms")
            .requiredLong("window_end_ms")
            .requiredString("vwap_value")
            .requiredLong("event_count")
            .requiredString("feature_name")
            .requiredString("feature_version")
            .requiredString("code_version")
            .endRecord();

    private final S3Client s3Client;
    private final String bucket;

    public FeatureParquetWriter(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    /**
     * Write a batch of feature events to a Parquet file and upload to MinIO.
     *
     * @param instrument the instrument symbol (e.g., "BTC-USDT")
     * @param events     the feature events to write
     * @return the S3 key where the file was stored
     * @throws StorageException if the write fails
     */
    public String write(String instrument, List<FeatureComputedEvent> events) {
        if (events.isEmpty()) {
            throw new IllegalArgumentException("Cannot write empty event list");
        }

        // Derive partition path from the first event's window start
        Instant windowStart = events.getFirst().windowStart();
        String partitionPath = buildPartitionPath(events.getFirst().featureName(),
                events.getFirst().featureVersion(), instrument, windowStart);
        String fileName = "part-00000-%s.parquet".formatted(java.util.UUID.randomUUID());
        String s3Key = partitionPath + "/" + fileName;

        java.nio.file.Path tempFile = null;
        try {
            // Write Parquet to a temp file
            tempFile = Files.createTempFile("muninn-parquet-", ".parquet");
            writeParquetFile(tempFile, events);

            // Upload to MinIO
            byte[] bytes = Files.readAllBytes(tempFile);
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Key)
                            .contentType("application/octet-stream")
                            .build(),
                    RequestBody.fromBytes(bytes)
            );

            log.atInfo()
                    .addKeyValue("s3Key", s3Key)
                    .addKeyValue("eventCount", events.size())
                    .addKeyValue("sizeBytes", bytes.length)
                    .log("Parquet file uploaded to MinIO");

            return s3Key;

        } catch (IOException e) {
            throw new StorageException("Failed to write Parquet file: " + s3Key, e);
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {
                    // Best-effort cleanup
                }
            }
        }
    }

    /**
     * Write feature events to a local Parquet file (for testing without S3).
     */
    static void writeParquetFile(java.nio.file.Path outputPath, List<FeatureComputedEvent> events) throws IOException {
        // Delete existing file if present (Parquet writer doesn't overwrite)
        Files.deleteIfExists(outputPath);

        // Use LocalOutputFile instead of Hadoop Path — avoids Java 21's
        // Subject.getSubject() removal that breaks Hadoop's UserGroupInformation
        LocalOutputFile outputFile = new LocalOutputFile(outputPath);

        try (org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer =
                     AvroParquetWriter.<GenericRecord>builder(outputFile)
                             .withSchema(FEATURE_SCHEMA)
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .withWriteMode(ParquetFileWriter.Mode.CREATE)
                             .build()) {

            for (FeatureComputedEvent event : events) {
                GenericRecord record = new GenericData.Record(FEATURE_SCHEMA);
                record.put("event_id", event.eventId().toString());
                record.put("event_time_ms", event.eventTime().toEpochMilli());
                record.put("window_start_ms", event.windowStart().toEpochMilli());
                record.put("window_end_ms", event.windowEnd().toEpochMilli());
                record.put("vwap_value", event.value().toPlainString());
                record.put("event_count", (long) event.inputEventIds().size());
                record.put("feature_name", event.featureName());
                record.put("feature_version", event.featureVersion());
                record.put("code_version", event.codeVersion());
                writer.write(record);
            }
        }
    }

    /**
     * Build a Hive-style partition path.
     */
    private String buildPartitionPath(String featureName, String featureVersion,
                                       String instrument, Instant windowStart) {
        var dt = windowStart.atZone(ZoneOffset.UTC);
        return "features.%s.%s/instrument=%s/year=%d/month=%02d/day=%02d/hour=%02d".formatted(
                featureName, featureVersion, instrument,
                dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour());
    }
}
