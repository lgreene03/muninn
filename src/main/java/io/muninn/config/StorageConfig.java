package io.muninn.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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

    @Bean
    public S3Properties s3Properties() {
        return new S3Properties("http://localhost:9002", "minioadmin", "minioadmin", "us-east-1");
    }

    @Bean
    public S3Client s3Client(S3Properties s3Properties) {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Properties.endpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3Properties.accessKey(), s3Properties.secretKey())
                ))
                .region(Region.of(s3Properties.region()))
                .forcePathStyle(true)
                .build();
    }

    @Bean
    public FeatureParquetWriter featureParquetWriter(S3Client s3Client, @Value("${muninn.storage.buckets.warehouse}") String warehouseBucket) {
        return new FeatureParquetWriter(s3Client, warehouseBucket);
    }
}
