package io.muninn.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The S3 client must be built from configuration, not from literals.
 *
 * <p>{@code S3Properties} is a {@code @ConfigurationProperties} record, and the
 * application carries {@code @ConfigurationPropertiesScan}, so Spring registers a
 * properly bound instance. {@code StorageConfig} also declared a {@code @Bean}
 * method returning one built from literal {@code http://localhost:9002}. A record is
 * immutable, so nothing can bind onto that second instance, and because the
 * {@code s3Client} parameter is named {@code s3Properties} it resolves to the
 * literal bean by name.
 *
 * <p>The consequence is environment-shaped. On a developer's host,
 * {@code localhost:9002} is the object store's published port, so everything works.
 * Inside a container it is the muninn container itself, so every S3 write fails,
 * which is why feature archival looked idle and why a raw-event recorder would
 * have recorded nothing in any containerised deployment.
 */
class StorageConfigTest {

    /** Stands in for the application's {@code @ConfigurationPropertiesScan}. */
    @EnableConfigurationProperties(StorageConfig.S3Properties.class)
    static class PropertiesScan {}

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(StorageConfig.class, PropertiesScan.class)
            .withPropertyValues("muninn.storage.buckets.warehouse=muninn-warehouse");

    @Test
    void configuredEndpointReachesTheS3Client() {
        runner.withPropertyValues("muninn.storage.s3.endpoint=http://objectstore.internal:9000")
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    S3Client client = ctx.getBean(S3Client.class);
                    assertThat(client.serviceClientConfiguration().endpointOverride())
                            .as("MUNINN_STORAGE_S3_ENDPOINT must reach the client, not a literal")
                            .contains(URI.create("http://objectstore.internal:9000"));
                });
    }

    @Test
    void configuredRegionReachesTheS3Client() {
        runner.withPropertyValues("muninn.storage.s3.region=eu-west-2")
                .run(ctx -> assertThat(ctx.getBean(S3Client.class).serviceClientConfiguration().region())
                        .isEqualTo(Region.EU_WEST_2));
    }

    @Test
    void exactlyOneS3PropertiesBeanExists() {
        // Two beans is the defect itself: one bound, one literal, resolved by name.
        runner.run(ctx -> assertThat(ctx).getBeans(StorageConfig.S3Properties.class).hasSize(1));
    }

    @Test
    void localDevelopmentDefaultsApplyWhenNothingIsConfigured() {
        // The record's compact constructor supplies the local defaults, so removing
        // the literal bean must not change behaviour for a developer with no config.
        runner.run(ctx -> {
            StorageConfig.S3Properties props = ctx.getBean(StorageConfig.S3Properties.class);
            assertThat(props.endpoint()).isEqualTo("http://localhost:9002");
            assertThat(props.accessKey()).isEqualTo("minioadmin");
            assertThat(props.region()).isEqualTo("us-east-1");
        });
    }
}
