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
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

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
