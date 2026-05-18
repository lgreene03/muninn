package io.muninn.storage.sink;

import io.muninn.shared.event.FeatureComputedEvent;

import java.util.List;

/**
 * Where the archival consumer flushes batches of computed features.
 *
 * <p>Two implementations exist:</p>
 * <ul>
 *   <li>{@link ParquetFeatureSink} — writes Hive-partitioned Parquet files
 *       to MinIO/S3. The default for every {@code local-*} profile and the
 *       interim path for {@code cloud-cheap}.</li>
 *   <li>{@link IcebergFeatureSink} — appends rows to an Iceberg table
 *       registered in the AWS Glue catalog. The {@code production-reference}
 *       choice; pairs with the Trino-backed query path
 *       (ADR-0006 + ADR-0007).</li>
 * </ul>
 *
 * <p>The Kafka archival consumer depends only on this interface. Backend
 * selection is property-driven via {@code muninn.archival.sink}
 * ({@code parquet} default, {@code iceberg} in production-reference).</p>
 *
 * <p>An ArchUnit rule enforces that the storage consumers stay free of
 * concrete sink references — see {@code ArchitectureRulesTest}.</p>
 */
public interface FeatureSink {

    /**
     * Persist a batch of feature events for a single instrument.
     *
     * <p>Implementations are responsible for partitioning and atomicity.
     * The batch is assumed to be non-empty; an empty list is the caller's
     * mistake and is rejected with {@link IllegalArgumentException}.</p>
     *
     * @param instrument symbol shared by every event in the batch
     * @param events     the feature events to persist; must be non-empty
     * @return a {@link SinkWriteResult} describing where the batch landed
     */
    SinkWriteResult write(String instrument, List<FeatureComputedEvent> events);

    /**
     * @return stable backend identifier ({@code "parquet"} or {@code "iceberg"}),
     *         surfaced as a metric tag so dashboards can distinguish sinks
     *         during migration windows.
     */
    String sinkId();

    /**
     * Where a write landed. {@code location} is the canonical reference an
     * operator would use to find the data — an S3 object key for Parquet,
     * a Iceberg table identifier + snapshot id for Iceberg.
     *
     * @param location  canonical reference to the written batch
     * @param rowCount  number of records persisted
     * @param sinkId    {@link #sinkId()} of the sink that produced the result
     */
    record SinkWriteResult(String location, int rowCount, String sinkId) {
        public SinkWriteResult {
            if (location == null || location.isBlank()) {
                throw new IllegalArgumentException("location is required");
            }
            if (rowCount < 0) throw new IllegalArgumentException("rowCount must be non-negative");
            if (sinkId == null || sinkId.isBlank()) {
                throw new IllegalArgumentException("sinkId is required");
            }
        }
    }
}
