package io.muninn.feature.checkpoint;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.muninn.shared.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.*;
import java.time.Instant;
import java.util.Optional;

/**
 * Manages feature-engine checkpoints in MinIO (S3-compatible).
 *
 * <p>Checkpoints are serialized {@link CheckpointState} objects stored at a known
 * path derived from the feature name, version, and watermark. This allows the engine
 * to resume from the most recent checkpoint after a restart.</p>
 *
 * <p>Path convention: {@code muninn-checkpoints/{featureName}/{featureVersion}/checkpoint-{watermarkEpochMs}.bin}</p>
 *
 * <p>Per DETERMINISTIC_REPLAY.md §Checkpoints: a checkpoint is only valid for the
 * exact code version that produced it.</p>
 */
public final class CheckpointManager {

    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);
    private static final String BUCKET = "muninn-checkpoints";

    private final S3Client s3Client;
    private final Timer checkpointDuration;

    public CheckpointManager(S3Client s3Client, MeterRegistry meterRegistry) {
        this.s3Client = s3Client;
        this.checkpointDuration = Timer.builder("muninn.feature.checkpoint.duration")
                .tag("feature", "vwap.1m")
                .register(meterRegistry);
    }

    /**
     * For unit testing without S3.
     */
    CheckpointManager() {
        this.s3Client = null;
        this.checkpointDuration = null;
    }

    /**
     * Write a checkpoint to MinIO.
     *
     * @param state the checkpoint state to persist
     * @return the S3 key where the checkpoint was stored
     * @throws StorageException if serialization or S3 write fails
     */
    public String write(CheckpointState state) {
        String key = buildKey(state.featureName(), state.featureVersion(), state.watermark());

        return checkpointDuration != null
                ? checkpointDuration.record(() -> doWrite(state, key))
                : doWrite(state, key);
    }

    private String doWrite(CheckpointState state, String key) {
        try {
            byte[] bytes = serialize(state);

            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .contentType("application/octet-stream")
                            .build(),
                    RequestBody.fromBytes(bytes)
            );

            log.atInfo()
                    .addKeyValue("feature", state.featureName())
                    .addKeyValue("version", state.featureVersion())
                    .addKeyValue("watermark", state.watermark())
                    .addKeyValue("key", key)
                    .addKeyValue("sizeBytes", bytes.length)
                    .log("Checkpoint written");

            return key;

        } catch (IOException e) {
            throw new StorageException("Failed to serialize checkpoint for " + state.featureName(), e);
        } catch (Exception e) {
            throw new StorageException("Failed to write checkpoint to MinIO: " + key, e);
        }
    }

    /**
     * Read a checkpoint from MinIO.
     *
     * @param featureName    the feature name
     * @param featureVersion the feature version (must match exactly)
     * @param watermark      the watermark of the checkpoint to restore
     * @return the checkpoint state, or empty if not found
     * @throws StorageException if deserialization fails
     */
    public Optional<CheckpointState> read(String featureName, String featureVersion, Instant watermark) {
        String key = buildKey(featureName, featureVersion, watermark);

        try {
            byte[] bytes = s3Client.getObject(
                    GetObjectRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .build(),
                    ResponseTransformer.toBytes()
            ).asByteArray();

            CheckpointState state = deserialize(bytes);

            // Critical invariant: checkpoint must be for the same code version
            if (!featureVersion.equals(state.featureVersion())) {
                log.atWarn()
                        .addKeyValue("expected", featureVersion)
                        .addKeyValue("actual", state.featureVersion())
                        .log("Checkpoint version mismatch — ignoring");
                return Optional.empty();
            }

            log.atInfo()
                    .addKeyValue("feature", featureName)
                    .addKeyValue("watermark", watermark)
                    .addKeyValue("key", key)
                    .log("Checkpoint restored");

            return Optional.of(state);

        } catch (NoSuchKeyException e) {
            log.atDebug()
                    .addKeyValue("key", key)
                    .log("No checkpoint found at key");
            return Optional.empty();
        } catch (Exception e) {
            throw new StorageException("Failed to read checkpoint from MinIO: " + key, e);
        }
    }

    /**
     * Restore the most recent checkpoint for a feature/version, if any exists.
     *
     * <p>Lists all checkpoint objects under the {@code {featureName}/{featureVersion}/}
     * prefix and selects the one with the highest watermark epoch (encoded in the key
     * by {@link #buildKey}). This is the entry point used by the feature engine on boot
     * to seed accumulated window state and the watermark before consuming, so a restart
     * does not silently drop partially-accumulated windows.</p>
     *
     * @param featureName    the feature name
     * @param featureVersion the feature version (must match exactly)
     * @return the latest checkpoint state, or empty if none exists
     * @throws StorageException if listing or deserialization fails
     */
    public Optional<CheckpointState> restoreLatest(String featureName, String featureVersion) {
        if (s3Client == null) {
            return Optional.empty();
        }

        String prefix = "%s/%s/".formatted(featureName, featureVersion);

        try {
            String latestKey = null;
            long latestEpoch = Long.MIN_VALUE;
            String continuationToken = null;

            do {
                ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                        .bucket(BUCKET)
                        .prefix(prefix);
                if (continuationToken != null) {
                    reqBuilder.continuationToken(continuationToken);
                }

                ListObjectsV2Response response = s3Client.listObjectsV2(reqBuilder.build());

                for (S3Object obj : response.contents()) {
                    long epoch = parseWatermarkEpoch(obj.key());
                    if (epoch > latestEpoch) {
                        latestEpoch = epoch;
                        latestKey = obj.key();
                    }
                }

                continuationToken = Boolean.TRUE.equals(response.isTruncated())
                        ? response.nextContinuationToken()
                        : null;
            } while (continuationToken != null);

            if (latestKey == null) {
                log.atInfo()
                        .addKeyValue("feature", featureName)
                        .addKeyValue("version", featureVersion)
                        .log("No checkpoint found to restore — starting cold");
                return Optional.empty();
            }

            return readByKey(featureName, featureVersion, latestKey);

        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw new StorageException("Failed to list checkpoints under prefix: " + prefix, e);
        }
    }

    private Optional<CheckpointState> readByKey(String featureName, String featureVersion, String key) {
        try {
            byte[] bytes = s3Client.getObject(
                    GetObjectRequest.builder()
                            .bucket(BUCKET)
                            .key(key)
                            .build(),
                    ResponseTransformer.toBytes()
            ).asByteArray();

            CheckpointState state = deserialize(bytes);

            if (!featureVersion.equals(state.featureVersion())) {
                log.atWarn()
                        .addKeyValue("expected", featureVersion)
                        .addKeyValue("actual", state.featureVersion())
                        .addKeyValue("key", key)
                        .log("Checkpoint version mismatch — ignoring");
                return Optional.empty();
            }

            log.atInfo()
                    .addKeyValue("feature", featureName)
                    .addKeyValue("watermark", state.watermark())
                    .addKeyValue("key", key)
                    .log("Latest checkpoint restored");

            return Optional.of(state);

        } catch (NoSuchKeyException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new StorageException("Failed to read checkpoint from MinIO: " + key, e);
        }
    }

    /**
     * Extract the watermark epoch millis encoded in a checkpoint key by {@link #buildKey}.
     * Returns {@link Long#MIN_VALUE} for keys that do not match the expected shape so they
     * are never selected as "latest".
     */
    static long parseWatermarkEpoch(String key) {
        int slash = key.lastIndexOf('/');
        String filename = slash >= 0 ? key.substring(slash + 1) : key;
        if (!filename.startsWith("checkpoint-") || !filename.endsWith(".bin")) {
            return Long.MIN_VALUE;
        }
        String epochPart = filename.substring("checkpoint-".length(), filename.length() - ".bin".length());
        try {
            return Long.parseLong(epochPart);
        } catch (NumberFormatException e) {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Serialize a checkpoint state to bytes using Java serialization.
     *
     * <p>Java serialization is the Phase 3 MVP choice. Migration to Avro is
     * planned for Phase 4 when cross-version checkpoint compatibility matters.</p>
     */
    static byte[] serialize(CheckpointState state) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(state);
            oos.flush();
            return bos.toByteArray();
        }
    }

    /**
     * Deserialize bytes back to a CheckpointState.
     */
    static CheckpointState deserialize(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (CheckpointState) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize checkpoint — class not found", e);
        }
    }

    private String buildKey(String featureName, String featureVersion, Instant watermark) {
        return "%s/%s/checkpoint-%d.bin".formatted(
                featureName, featureVersion, watermark.toEpochMilli());
    }
}
