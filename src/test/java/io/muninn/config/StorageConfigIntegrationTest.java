package io.muninn.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Bytes written through muninn's S3 client must come back as the same bytes.
 *
 * <p>Uses {@link StorageConfig#newS3Client}, the factory production uses, against the
 * object store the stack runs (SeaweedFS). A contract test that validates the server
 * with a different client proves nothing about this one: an earlier check passed 8 of
 * 8 using Python's boto3, which does not use {@code aws-chunked} encoding, while the
 * Java SDK's default corrupted every object.
 */
@Tag("integration")
@Testcontainers
class StorageConfigIntegrationTest {

    private static final String S3_CONFIG = """
            {"identities":[{"name":"it","credentials":[{"accessKey":"it","secretKey":"it-secret"}],
              "actions":["Admin","Read","List","Tagging","Write"]}]}""";

    @Container
    static final GenericContainer<?> objectstore = new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:3.97"))
            .withCopyToContainer(Transferable.of(S3_CONFIG), "/etc/seaweedfs/s3.json")
            .withCommand("server", "-dir=/data", "-ip.bind=0.0.0.0", "-s3", "-s3.port=9000",
                    "-s3.config=/etc/seaweedfs/s3.json", "-master.volumeSizeLimitMB=64", "-volume.max=0")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/status").forPort(9000).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2));

    private static String endpoint;

    @BeforeAll
    static void bucket() {
        endpoint = "http://" + objectstore.getHost() + ":" + objectstore.getMappedPort(9000);
        try (S3Client s3 = production()) {
            s3.createBucket(b -> b.bucket("roundtrip"));
        }
    }

    private static S3Client production() {
        return StorageConfig.newS3Client(new StorageConfig.S3Properties(endpoint, "it", "it-secret", "us-east-1"));
    }

    private static byte[] gzip(String text) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(text.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }

    @Test
    void productionClientRoundTripsGzipByteForByte() throws Exception {
        byte[] body = gzip("{\"offset\":1,\"value\":{\"size\":0.00150}}\n".repeat(200));
        try (S3Client s3 = production()) {
            s3.putObject(r -> r.bucket("roundtrip").key("a/part.jsonl.gz").contentType("application/gzip"),
                    RequestBody.fromBytes(body));
            byte[] got = s3.getObjectAsBytes(r -> r.bucket("roundtrip").key("a/part.jsonl.gz")).asByteArray();
            assertThat(got).as("stored bytes must equal written bytes").isEqualTo(body);
        }
    }

    @Test
    void productionClientRoundTripsALargeObject() throws Exception {
        // Large enough to be sent in several chunks, which is where framing would show.
        byte[] body = gzip("x".repeat(4 * 1024 * 1024) + "y");
        try (S3Client s3 = production()) {
            s3.putObject(r -> r.bucket("roundtrip").key("b/big.gz"), RequestBody.fromBytes(body));
            assertThat(s3.getObjectAsBytes(r -> r.bucket("roundtrip").key("b/big.gz")).asByteArray()).isEqualTo(body);
        }
    }

    @Test
    void theSdkDefaultWouldHaveCorruptedTheSameBytes() throws Exception {
        // The counterfactual, so this suite provably detects the defect rather than
        // passing vacuously. If a future SeaweedFS decodes aws-chunked trailers, this
        // test starts failing, and the WHEN_REQUIRED setting can be reconsidered.
        byte[] body = gzip("{\"offset\":1}\n".repeat(50));
        try (S3Client sdkDefault = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("it", "it-secret")))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build()) {
            sdkDefault.putObject(r -> r.bucket("roundtrip").key("c/default.gz"), RequestBody.fromBytes(body));
            byte[] got = sdkDefault.getObjectAsBytes(r -> r.bucket("roundtrip").key("c/default.gz")).asByteArray();
            assertThat(got).as("SDK default stores the chunk-framed wire body").isNotEqualTo(body);
            assertThat(new String(got, StandardCharsets.ISO_8859_1)).contains("chunk-signature=");
        }
    }
}
