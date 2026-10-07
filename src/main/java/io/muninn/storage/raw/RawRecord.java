package io.muninn.storage.raw;

/**
 * One Kafka record as the recorder sees it: its coordinates and the exact value bytes.
 *
 * <p>The value is kept as the bytes the broker holds, never as a deserialised event.
 * Round-tripping through a typed object is lossy in ways that matter for history: a
 * venue quoting {@code 0.00150} carries its precision in that trailing zero, and a
 * double, or any re-serialisation, can drop it. Capture is lossless here and analytic
 * formats are derived later.
 *
 * <p>{@code value} is a byte array, so this record compares by reference. Nothing
 * relies on record equality.
 *
 * @param topic     source topic
 * @param partition Kafka partition, which scopes {@code offset}
 * @param offset    Kafka offset; with {@code partition} it identifies the record exactly
 * @param timestamp Kafka record timestamp in epoch milliseconds
 * @param key       record key, possibly null
 * @param value     record value bytes, possibly null for a tombstone
 */
public record RawRecord(String topic, int partition, long offset, long timestamp, String key, byte[] value) {}
