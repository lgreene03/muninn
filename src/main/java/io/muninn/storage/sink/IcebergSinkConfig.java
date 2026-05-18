package io.muninn.storage.sink;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Iceberg sink configuration. Inactive unless {@code muninn.archival.sink=iceberg}.
 *
 * <p>Defaults assume a local development setup. The production-reference Helm
 * chart overrides {@code catalogType=glue}, {@code warehouse=s3://...}, and
 * {@code awsRegion} to match the Terraform-provisioned warehouse + Glue
 * catalog (see {@code local-infra/terraform/aws/modules/s3_iceberg}).</p>
 *
 * @param catalogType    one of {@code "glue"} or {@code "hadoop"}; {@code glue}
 *                       is the production-reference choice (ADR-0005).
 * @param warehouse      catalog warehouse URI (e.g., {@code s3://muninn-warehouse}).
 * @param glueDatabase   Glue database name; ignored when {@code catalogType=hadoop}.
 * @param awsRegion      AWS region for the Glue + S3 clients.
 * @param schema         Iceberg schema namespace within the catalog (e.g., {@code muninn}).
 */
@ConfigurationProperties(prefix = "muninn.archival.iceberg")
public record IcebergSinkConfig(
        String catalogType,
        String warehouse,
        String glueDatabase,
        String awsRegion,
        String schema
) {

    public IcebergSinkConfig {
        if (catalogType == null || catalogType.isBlank()) catalogType = "glue";
        if (warehouse == null || warehouse.isBlank()) warehouse = "s3://muninn-warehouse";
        if (glueDatabase == null || glueDatabase.isBlank()) glueDatabase = "muninn";
        if (awsRegion == null || awsRegion.isBlank()) awsRegion = "us-east-1";
        if (schema == null || schema.isBlank()) schema = "muninn";
    }
}
