package io.muninn.storage.sink;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.storage.FeatureParquetWriter;

import java.util.List;

/**
 * {@link FeatureSink} backed by {@link FeatureParquetWriter}.
 *
 * <p>Behaviour is the same as the pre-abstraction archival path: a Hive-style
 * partition layout and an S3/MinIO upload. Existing operators see no change.</p>
 *
 * <p>The Iceberg sink (ADR-0007) is selected by setting
 * {@code muninn.archival.sink=iceberg}; this remains the default.</p>
 */
public final class ParquetFeatureSink implements FeatureSink {

    public static final String SINK_ID = "parquet";

    private final FeatureParquetWriter writer;

    public ParquetFeatureSink(FeatureParquetWriter writer) {
        this.writer = writer;
    }

    @Override
    public String sinkId() {
        return SINK_ID;
    }

    @Override
    public SinkWriteResult write(String instrument, List<FeatureComputedEvent> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("events must be non-empty");
        }
        String s3Key = writer.write(instrument, events);
        return new SinkWriteResult(s3Key, events.size(), SINK_ID);
    }
}
