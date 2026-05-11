package io.muninn.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Configuration
public class StorageConfig {

    @Value("${muninn.storage.s3.endpoint:http://localhost:9000}")
    private String s3Endpoint;

    @Value("${muninn.storage.s3.access-key:minioadmin}")
    private String accessKey;

    @Value("${muninn.storage.s3.secret-key:minioadmin}")
    private String secretKey;

    @Value("${muninn.storage.s3.region:us-east-1}")
    private String region;

    @Bean
    public S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                ))
                .region(Region.of(region))
                .forcePathStyle(true)
                .build();
    }
}
