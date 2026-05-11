package io.muninn.shared.time;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link UUIDv7}.
 */
class UUIDv7Test {

    @Test
    void generate_producesVersion7UUID() {
        UUID uuid = UUIDv7.generate();
        assertThat(uuid.version()).isEqualTo(7);
    }

    @Test
    void generate_producesVariant2UUID() {
        UUID uuid = UUIDv7.generate();
        assertThat(uuid.variant()).isEqualTo(2);
    }

    @Test
    void generate_withTimestamp_embedsTimestamp() {
        long epochMillis = 1715000000000L;
        UUID uuid = UUIDv7.generate(epochMillis);

        Instant extracted = UUIDv7.extractTimestamp(uuid);
        assertThat(extracted.toEpochMilli()).isEqualTo(epochMillis);
    }

    @Test
    void generate_multipleInSameThread_areUnique() {
        // The no-arg generate() uses wall clock + monotonic counter
        Set<UUID> uuids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            uuids.add(UUIDv7.generate());
        }
        assertThat(uuids).hasSize(1000);
    }

    @Test
    void generate_producesUniqueUUIDs() {
        Set<UUID> uuids = new HashSet<>();
        for (int i = 0; i < 10_000; i++) {
            uuids.add(UUIDv7.generate());
        }
        assertThat(uuids).hasSize(10_000);
    }

    @Test
    void extractTimestamp_roundTrips() {
        Instant now = Instant.now();
        UUID uuid = UUIDv7.generate(now.toEpochMilli());
        Instant extracted = UUIDv7.extractTimestamp(uuid);
        assertThat(extracted.toEpochMilli()).isEqualTo(now.toEpochMilli());
    }
}
