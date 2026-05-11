package io.muninn.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

@Component
public class MinioObjectStore implements ObjectStore {

    private static final Logger log = LoggerFactory.getLogger(MinioObjectStore.class);

    private final S3Client s3Client;

    public MinioObjectStore(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public void put(String bucket, String key, Path file) throws IOException {
        s3Client.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).build(),
                RequestBody.fromFile(file)
        );
        log.debug("Uploaded {} to s3://{}/{}", file, bucket, key);
    }

    @Override
    public InputStream get(String bucket, String key) {
        return s3Client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(key).build()
        );
    }

    @Override
    public List<String> list(String bucket, String prefix) {
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build()
        );
        return response.contents().stream()
                .map(S3Object::key)
                .toList();
    }

    @Override
    public void delete(String bucket, String key) {
        s3Client.deleteObject(
                DeleteObjectRequest.builder().bucket(bucket).key(key).build()
        );
        log.debug("Deleted s3://{}/{}", bucket, key);
    }
}
