package io.muninn.storage.sink;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavior tests for the contract surface of {@link IcebergFeatureSink}.
 *
 * <p>End-to-end writes (Parquet append + Iceberg snapshot commit) need a
 * filesystem catalog with a real {@code FileIO}, which is heavy to set up
 * in a unit test — that path is tracked for a follow-up Testcontainers
 * integration test using MinIO + a HadoopCatalog. Here we pin the
 * <em>table-naming contract</em> from ADR-0006 (which the Trino query
 * backend depends on) and surface-level input validation.</p>
 *
 * <p>Uses a hand-rolled {@link Catalog} stub rather than Mockito so the
 * tests run on JDKs that restrict byte-buddy's self-attach mechanism.</p>
 */
class IcebergFeatureSinkTest {

    @Test
    void sinkId_isIceberg() {
        IcebergFeatureSink sink = new IcebergFeatureSink(new NullCatalog(), defaultConfig());
        assertThat(sink.sinkId()).isEqualTo("iceberg");
    }

    @Test
    void tableIdentifierFor_matchesAdr0006Convention() {
        IcebergFeatureSink sink = new IcebergFeatureSink(new NullCatalog(), defaultConfig());

        TableIdentifier id = sink.tableIdentifierFor("vwap.1m", "v1");

        // Dots replaced by underscores, lower-cased, prefixed with features_.
        // featureVersion is not part of the table name — see comment in
        // IcebergFeatureSink#tableIdentifierFor.
        assertThat(id.name()).isEqualTo("features_vwap_1m");
        assertThat(id.namespace().level(0)).isEqualTo("muninn");
    }

    @Test
    void tableIdentifierFor_handlesDashesAndUppercase() {
        IcebergFeatureSink sink = new IcebergFeatureSink(new NullCatalog(), defaultConfig());

        assertThat(sink.tableIdentifierFor("VPIN", "v1").name()).isEqualTo("features_vpin");
        assertThat(sink.tableIdentifierFor("ob-imbalance.fast", "v1").name())
                .isEqualTo("features_ob_imbalance_fast");
    }

    @Test
    void tableIdentifier_respectsCustomSchema() {
        IcebergSinkConfig custom = new IcebergSinkConfig(
                "glue", "s3://w", "muninn", "us-east-1", "research");
        IcebergFeatureSink sink = new IcebergFeatureSink(new NullCatalog(), custom);

        TableIdentifier id = sink.tableIdentifierFor("vwap.1m", "v1");
        assertThat(id.namespace().level(0)).isEqualTo("research");
    }

    @Test
    void write_rejectsEmptyBatch() {
        IcebergFeatureSink sink = new IcebergFeatureSink(new NullCatalog(), defaultConfig());

        assertThatThrownBy(() -> sink.write("BTC-USDT", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty");
    }

    @Test
    void config_appliesDefaults() {
        IcebergSinkConfig config = new IcebergSinkConfig(null, null, null, null, null);

        assertThat(config.catalogType()).isEqualTo("glue");
        assertThat(config.warehouse()).isEqualTo("s3://muninn-warehouse");
        assertThat(config.glueDatabase()).isEqualTo("muninn");
        assertThat(config.awsRegion()).isEqualTo("us-east-1");
        assertThat(config.schema()).isEqualTo("muninn");
    }

    private static IcebergSinkConfig defaultConfig() {
        return new IcebergSinkConfig("glue", "s3://muninn-warehouse", "muninn", "us-east-1", "muninn");
    }

    /**
     * No-op catalog stub. Only the table-naming and validation paths in
     * {@link IcebergFeatureSink} are exercised here — none of these tests
     * trigger a real write, so the catalog's I/O methods don't need to
     * be implemented.
     */
    private static final class NullCatalog implements Catalog {
        @Override public String name() { return "null"; }
        @Override public List<TableIdentifier> listTables(Namespace namespace) { return List.of(); }
        @Override public boolean dropTable(TableIdentifier identifier, boolean purge) { return false; }
        @Override public void renameTable(TableIdentifier from, TableIdentifier to) { }
        @Override public Table loadTable(TableIdentifier identifier) {
            throw new UnsupportedOperationException("not used");
        }
        @Override public Table createTable(
                TableIdentifier identifier, Schema schema, PartitionSpec spec,
                String location, Map<String, String> properties) {
            throw new UnsupportedOperationException("not used");
        }
    }
}
