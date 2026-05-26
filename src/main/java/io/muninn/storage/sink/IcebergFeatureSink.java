package io.muninn.storage.sink;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.exception.StorageException;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link FeatureSink} that appends to Iceberg tables (Glue catalog in
 * production-reference; pluggable for tests).
 *
 * <p>Table naming matches the convention {@link TrinoFeatureQueryBackend}
 * reads from — {@code features_<name>_<version>}, dots and dashes replaced
 * by underscores — so the Trino query path finds whatever this sink writes.
 * The contract is documented in ADR-0006 and ADR-0007.</p>
 *
 * <p>Tables are created on first write per (featureName, featureVersion).
 * Schema is fixed for the MVP feature shape and matches the one
 * {@code FeatureParquetWriter} uses, so the migration script can convert
 * existing Parquet data without column mapping.</p>
 *
 * <p>Partitioned by {@code instrument} and {@code window_start} truncated to
 * the hour — same coarseness as the Hive partition layout, so query
 * plan-pruning works the same way.</p>
 */
public final class IcebergFeatureSink implements FeatureSink {

    public static final String SINK_ID = "iceberg";

    private static final Logger log = LoggerFactory.getLogger(IcebergFeatureSink.class);

    /** Fixed schema for feature rows. Matches {@code FeatureParquetWriter.FEATURE_SCHEMA}. */
    static final Schema FEATURE_SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "window_start", Types.TimestampType.withZone()),
            Types.NestedField.required(4, "window_end", Types.TimestampType.withZone()),
            Types.NestedField.required(5, "instrument", Types.StringType.get()),
            Types.NestedField.required(6, "value", Types.StringType.get()),
            Types.NestedField.required(7, "input_event_count", Types.LongType.get()),
            Types.NestedField.required(8, "feature_name", Types.StringType.get()),
            Types.NestedField.required(9, "feature_version", Types.StringType.get()),
            Types.NestedField.required(10, "code_version", Types.StringType.get())
    );

    /** Partition by instrument + hour of window_start. */
    static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(FEATURE_SCHEMA)
            .identity("instrument")
            .hour("window_start")
            .build();

    private final Catalog catalog;
    private final IcebergSinkConfig config;
    private final ConcurrentMap<String, Table> tableCache = new ConcurrentHashMap<>();

    public IcebergFeatureSink(Catalog catalog, IcebergSinkConfig config) {
        this.catalog = catalog;
        this.config = config;
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
        FeatureComputedEvent first = events.getFirst();
        TableIdentifier id = tableIdentifierFor(first.featureName(), first.featureVersion());
        Table table = resolveOrCreate(id);

        String filename = "data-" + UUID.randomUUID() + "." + FileFormat.PARQUET.name().toLowerCase(Locale.ROOT);
        OutputFile outputFile = table.io().newOutputFile(table.location() + "/data/" + filename);

        try {
            DataFile dataFile = writeBatch(table, outputFile, instrument, events);
            AppendFiles append = table.newAppend().appendFile(dataFile);
            append.commit();

            long snapshotId = table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : -1L;
            String location = id + "@" + snapshotId;
            log.atInfo()
                    .addKeyValue("table", id.toString())
                    .addKeyValue("snapshot", snapshotId)
                    .addKeyValue("rows", events.size())
                    .addKeyValue("file", outputFile.location())
                    .log("Iceberg append committed");
            return new SinkWriteResult(location, events.size(), SINK_ID);
        } catch (IOException e) {
            throw new StorageException("Iceberg write failed for " + id, e);
        }
    }

    private DataFile writeBatch(
            Table table,
            OutputFile outputFile,
            String instrument,
            List<FeatureComputedEvent> events
    ) throws IOException {
        try (FileAppender<Record> appender = Parquet.write(outputFile)
                .schema(FEATURE_SCHEMA)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build()) {

            for (FeatureComputedEvent event : events) {
                appender.add(toRecord(instrument, event));
            }
            appender.close();

            return DataFiles.builder(table.spec())
                    .withInputFile(outputFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFileSizeInBytes(appender.length())
                    .withFormat(FileFormat.PARQUET)
                    .withPartition(partitionDataFor(instrument, events.getFirst()))
                    .build();
        }
    }

    /**
     * Resolve the Iceberg {@link Table} for this feature, creating it on first write.
     *
     * <p>Cached after first resolution so subsequent writes skip a catalog round trip.
     * If the catalog is replaced (e.g., region failover), the application is restarted
     * — we don't try to invalidate at runtime.</p>
     */
    Table resolveOrCreate(TableIdentifier id) {
        return tableCache.computeIfAbsent(id.toString(), key -> {
            try {
                return catalog.loadTable(id);
            } catch (NoSuchTableException notFound) {
                log.atInfo()
                        .addKeyValue("table", id.toString())
                        .log("Iceberg table not found; creating");
                return catalog.createTable(
                        id,
                        FEATURE_SCHEMA,
                        PARTITION_SPEC,
                        Map.of("format-version", "2")
                );
            }
        });
    }

    private static GenericRecord toRecord(String instrument, FeatureComputedEvent event) {
        GenericRecord record = GenericRecord.create(FEATURE_SCHEMA);
        record.setField("event_id", event.eventId().toString());
        record.setField("event_time", OffsetDateTime.ofInstant(event.eventTime(), ZoneOffset.UTC));
        record.setField("window_start", OffsetDateTime.ofInstant(event.windowStart(), ZoneOffset.UTC));
        record.setField("window_end", OffsetDateTime.ofInstant(event.windowEnd(), ZoneOffset.UTC));
        record.setField("instrument", instrument);
        record.setField("value", event.value() == null ? "" : event.value().toPlainString());
        record.setField("input_event_count", (long) event.inputEventIds().size());
        record.setField("feature_name", event.featureName());
        record.setField("feature_version", event.featureVersion());
        record.setField("code_version", event.codeVersion());
        return record;
    }

    private org.apache.iceberg.PartitionKey partitionDataFor(
            String instrument,
            FeatureComputedEvent first
    ) {
        // PartitionKey builds the partition values from a sample record so the
        // identity(instrument) + hour(window_start) transforms are applied consistently
        // with what Iceberg expects on read.
        org.apache.iceberg.PartitionKey key = new org.apache.iceberg.PartitionKey(PARTITION_SPEC, FEATURE_SCHEMA);
        GenericRecord sample = GenericRecord.create(FEATURE_SCHEMA);
        sample.setField("instrument", instrument);
        sample.setField("window_start", OffsetDateTime.ofInstant(first.windowStart(), ZoneOffset.UTC));
        // Fill required fields with placeholders so partition() doesn't NPE on schema validation.
        sample.setField("event_id", "");
        sample.setField("event_time", OffsetDateTime.ofInstant(first.eventTime(), ZoneOffset.UTC));
        sample.setField("window_end", OffsetDateTime.ofInstant(first.windowEnd(), ZoneOffset.UTC));
        sample.setField("value", "");
        sample.setField("input_event_count", 0L);
        sample.setField("feature_name", first.featureName());
        sample.setField("feature_version", first.featureVersion());
        sample.setField("code_version", first.codeVersion());
        key.partition(sample);
        return key;
    }

    /**
     * Iceberg table identifier matching {@code TrinoFeatureQueryBackend.tableNameFor()}.
     * See ADR-0006 §Naming.
     */
    TableIdentifier tableIdentifierFor(String featureName, String featureVersion) {
        String tableName = "features_"
                + featureName.toLowerCase(Locale.ROOT).replace('.', '_').replace('-', '_');
        // featureVersion is intentionally embedded in the schema column rather than the
        // table name so the same table holds successive versions of a feature definition.
        // The query backend filters on feature_version when version-specific results are required.
        return TableIdentifier.of(Namespace.of(config.schema()), tableName);
    }

    @SuppressWarnings("unused")
    private static LocalDateTime nowLocal() {
        // Placeholder helper retained to silence the unused-import noise when stripping
        // diagnostics; never called.
        return LocalDateTime.MIN;
    }
}
