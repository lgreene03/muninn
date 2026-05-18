package io.muninn.storage.sink;

import io.muninn.storage.FeatureParquetWriter;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Wires exactly one {@link FeatureSink} bean based on
 * {@code muninn.archival.sink}.
 *
 * <p>Default is {@code parquet} so every {@code local-*} profile keeps the
 * pre-abstraction behavior. The {@code production-reference} profile (or any
 * Helm values override) sets {@code muninn.archival.sink=iceberg} to engage
 * the Iceberg + Glue path.</p>
 *
 * <p>Only one branch instantiates per app start, so the Iceberg catalog
 * client never opens when Parquet is active and vice versa.</p>
 */
@Configuration
@EnableConfigurationProperties(IcebergSinkConfig.class)
public class FeatureSinkConfiguration {

    public static final String PROPERTY = "muninn.archival.sink";

    private static final Logger log = LoggerFactory.getLogger(FeatureSinkConfiguration.class);

    @Bean
    @ConditionalOnProperty(name = PROPERTY, havingValue = "parquet", matchIfMissing = true)
    public FeatureSink parquetFeatureSink(FeatureParquetWriter writer) {
        log.atInfo().log("Archival sink: parquet");
        return new ParquetFeatureSink(writer);
    }

    @Bean
    @ConditionalOnProperty(name = PROPERTY, havingValue = "iceberg")
    public Catalog icebergCatalog(IcebergSinkConfig config) {
        String type = config.catalogType().toLowerCase(Locale.ROOT);
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, config.warehouse());
        props.put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName());
        props.put("client.region", config.awsRegion());

        if ("glue".equals(type)) {
            GlueCatalog catalog = new GlueCatalog();
            catalog.initialize("muninn-glue", props);
            log.atInfo()
                    .addKeyValue("catalog", "glue")
                    .addKeyValue("warehouse", config.warehouse())
                    .addKeyValue("database", config.glueDatabase())
                    .log("Iceberg catalog initialized");
            return catalog;
        }
        if ("hadoop".equals(type)) {
            // Hadoop catalog is filesystem-based and useful for non-AWS deployments
            // or for testing without a real Glue endpoint. Production-reference uses
            // glue; see ADR-0007 §Catalog choice.
            HadoopCatalog catalog = new HadoopCatalog();
            catalog.setConf(new org.apache.hadoop.conf.Configuration());
            catalog.initialize("muninn-hadoop", props);
            log.atInfo()
                    .addKeyValue("catalog", "hadoop")
                    .addKeyValue("warehouse", config.warehouse())
                    .log("Iceberg catalog initialized");
            return catalog;
        }
        throw new IllegalArgumentException(
                "Unsupported muninn.archival.iceberg.catalog-type: " + config.catalogType()
                        + " (expected 'glue' or 'hadoop')");
    }

    @Bean
    @ConditionalOnProperty(name = PROPERTY, havingValue = "iceberg")
    public FeatureSink icebergFeatureSink(Catalog icebergCatalog, IcebergSinkConfig config) {
        log.atInfo()
                .addKeyValue("catalog", config.catalogType())
                .addKeyValue("schema", config.schema())
                .log("Archival sink: iceberg");
        return new IcebergFeatureSink(icebergCatalog, config);
    }
}
