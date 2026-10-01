package io.muninn.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import org.springframework.beans.factory.annotation.Value;
import io.muninn.storage.FeatureParquetWriter;

/**
 * S3/MinIO storage configuration using {@code @ConfigurationProperties}.
 */
@Configuration
public class StorageConfig {

    @ConfigurationProperties(prefix = "muninn.storage.s3")
    public record S3Properties(
            String endpoint,
            String accessKey,
            String secretKey,
            String region
    ) {
        public S3Properties {
            if (endpoint == null || endpoint.isBlank()) endpoint = "http://localhost:9002";
            if (accessKey == null || accessKey.isBlank()) accessKey = "minioadmin";
            if (secretKey == null || secretKey.isBlank()) secretKey = "minioadmin";
            if (region == null || region.isBlank()) region = "us-east-1";
        }
    }

    // There is deliberately no @Bean method for S3Properties. The application's
    // @ConfigurationPropertiesScan registers one bound from `muninn.storage.s3.*`,
    // and the compact constructor above supplies the local-development defaults.
    // A literal @Bean here used to create a second instance that nothing could
    // bind onto (records are immutable) and that won injection by parameter name,
    // so MUNINN_STORAGE_S3_ENDPOINT was silently ignored. On a host that still
    // worked, because localhost:9002 is the published port; inside a container it
    // pointed muninn at itself and every S3 write failed. See StorageConfigTest.

    @Bean
    public S3Client s3Client(S3Properties s3Properties) {
        return newS3Client(s3Properties);
    }

    /**
     * The one place an S3 client is built, so tests exercise exactly what production runs.
     *
     * <p><b>Checksums are sent only when required.</b> Since AWS SDK for Java 2.30 the
     * default is to attach a CRC checksum to every upload using {@code aws-chunked}
     * trailing encoding. An S3-compatible store that does not decode that framing
     * stores the raw chunked wire body AS the object. Measured against SeaweedFS: a
     * 75-byte gzip upload came back as 369 bytes beginning
     * {@code 4b;chunk-signature=...} ({@code 4b} is 75 in hex, the chunk size), with
     * both the PUT and the GET reporting success. Nothing errors; the data is simply
     * wrong. With {@code WHEN_REQUIRED} the same upload round-trips byte-identically.
     *
     * <p>This is the conservative setting for any S3-compatible store and is fully
     * supported by AWS S3 itself. Integrity is still protected: S3 verifies the
     * Content-MD5 and signature, and the recorder's own tests compare bytes end to end.
     */
    public static S3Client newS3Client(S3Properties s3Properties) {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Properties.endpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3Properties.accessKey(), s3Properties.secretKey())
                ))
                .region(Region.of(s3Properties.region()))
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
    }

    @Bean
    public FeatureParquetWriter featureParquetWriter(S3Client s3Client, @Value("${muninn.storage.buckets.warehouse}") String warehouseBucket) {
        return new FeatureParquetWriter(s3Client, warehouseBucket);
    }
}
