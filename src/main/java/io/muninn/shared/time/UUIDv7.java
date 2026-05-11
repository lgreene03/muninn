package io.muninn.shared.time;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates UUIDv7 identifiers per RFC 9562.
 *
 * <p>UUIDv7 is time-ordered: the first 48 bits are a Unix epoch millisecond timestamp,
 * making these UUIDs sortable by creation time. This is the required identity format
 * for all Muninn events (see DOMAIN_MODEL.md).</p>
 *
 * <p>Thread-safe. Uses a monotonic counter within the same millisecond to guarantee
 * strict ordering even under burst traffic.</p>
 */
public final class UUIDv7 {

    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Tracks the last timestamp used, to ensure monotonicity within the same millisecond.
     */
    private static final AtomicLong LAST_TIMESTAMP = new AtomicLong(0);
    private static final AtomicLong COUNTER = new AtomicLong(0);

    private UUIDv7() {
        // Utility class — no instantiation
    }

    /**
     * Generate a new UUIDv7 using the wall clock.
     * Suitable for ingestion-time event ID assignment (not inside feature-engine code).
     *
     * <p>Guarantees monotonicity: if called multiple times within the same millisecond,
     * the counter portion increments to maintain sort order.</p>
     */
    public static UUID generate() {
        long epochMillis = System.currentTimeMillis();
        long lastTs = LAST_TIMESTAMP.get();
        long counter;

        if (epochMillis > lastTs) {
            if (LAST_TIMESTAMP.compareAndSet(lastTs, epochMillis)) {
                COUNTER.set(0);
                counter = 0;
            } else {
                counter = COUNTER.incrementAndGet();
                epochMillis = LAST_TIMESTAMP.get();
            }
        } else {
            counter = COUNTER.incrementAndGet();
            epochMillis = LAST_TIMESTAMP.get();
        }

        return buildUuid(epochMillis, counter);
    }

    /**
     * Generate a new UUIDv7 for a given epoch-millisecond timestamp.
     * Use this overload for deterministic ID generation during replay or testing.
     * Always embeds the provided timestamp — no monotonicity enforcement against
     * wall-clock calls.
     *
     * @param epochMillis the Unix epoch millisecond timestamp to embed
     * @return a new UUIDv7 with the given timestamp
     */
    public static UUID generate(long epochMillis) {
        long counter = RANDOM.nextLong() & 0xFFF; // 12-bit random sub-ms component
        return buildUuid(epochMillis, counter);
    }

    private static UUID buildUuid(long epochMillis, long counter) {
        // Layout per RFC 9562 §5.7:
        // Bits  0-47: 48-bit Unix timestamp (ms)
        // Bits 48-51: version (0b0111 = 7)
        // Bits 52-63: 12-bit sub-millisecond counter / random
        // Bits 64-65: variant (0b10)
        // Bits 66-127: 62-bit random

        long msb = (epochMillis & 0xFFFF_FFFF_FFFFL) << 16; // 48-bit timestamp → top 48 bits
        msb |= 0x7000L;                                       // version 7
        msb |= (counter & 0xFFF);                              // 12-bit counter

        long lsb = RANDOM.nextLong();
        lsb = (lsb & 0x3FFF_FFFF_FFFF_FFFFL) | 0x8000_0000_0000_0000L; // variant 10

        return new UUID(msb, lsb);
    }

    /**
     * Extract the embedded timestamp from a UUIDv7.
     *
     * @param uuid a UUIDv7
     * @return the embedded timestamp as an {@link Instant}
     */
    public static Instant extractTimestamp(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long epochMillis = msb >>> 16;
        return Instant.ofEpochMilli(epochMillis);
    }
}
