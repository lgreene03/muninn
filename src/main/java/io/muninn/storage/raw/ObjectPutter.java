package io.muninn.storage.raw;

/**
 * Writes one object. The narrow boundary between the recorder and object storage, so the
 * grouping and offset logic can be tested without an S3 client.
 *
 * <p>Implementations must either store the object durably or throw. Returning normally
 * is taken to mean the bytes are safe, and the recorder commits Kafka offsets on the
 * strength of it.
 */
@FunctionalInterface
public interface ObjectPutter {

    void put(String key, byte[] body);
}
