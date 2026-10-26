package io.muninn.storage.raw;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * {@link ObjectPutter} backed by S3 (SeaweedFS locally, any S3-compatible store elsewhere).
 *
 * <p>The content type is {@code application/gzip} and no {@code Content-Encoding} is
 * set. With {@code Content-Encoding: gzip} some clients decompress transparently on
 * download, so a reader expecting the {@code .jsonl.gz} bytes the key promises would
 * receive plain text instead.
 */
public final class S3ObjectPutter implements ObjectPutter {

    private final S3Client s3;
    private final String bucket;

    public S3ObjectPutter(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void put(String key, byte[] body) {
        // putObject throws on any failure; a normal return means the store accepted it.
        s3.putObject(
                PutObjectRequest.builder().bucket(bucket).key(key).contentType("application/gzip").build(),
                RequestBody.fromBytes(body));
    }
}
