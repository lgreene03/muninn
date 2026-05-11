package io.muninn.feature.checkpoint;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link CheckpointState} and {@link CheckpointManager} serialization.
 *
 * <p>Tests serialization round-trip without S3 — validates that checkpoint state
 * can be serialized and deserialized faithfully.</p>
 */
class CheckpointManagerTest {

    private static final Instant WATERMARK = Instant.parse("2026-05-11T14:05:00Z");

    @Test
    void serializeDeserialize_roundTrips() throws IOException {
        CheckpointState original = buildCheckpoint();

        byte[] bytes = CheckpointManager.serialize(original);
        CheckpointState restored = CheckpointManager.deserialize(bytes);

        assertThat(restored.featureName()).isEqualTo(original.featureName());
        assertThat(restored.featureVersion()).isEqualTo(original.featureVersion());
        assertThat(restored.watermark()).isEqualTo(original.watermark());
        assertThat(restored.windowStates()).hasSize(2);
        assertThat(restored.consumerOffsets()).containsEntry(0, 1500L);
    }

    @Test
    void serializeDeserialize_preservesWindowState() throws IOException {
        CheckpointState original = buildCheckpoint();

        byte[] bytes = CheckpointManager.serialize(original);
        CheckpointState restored = CheckpointManager.deserialize(bytes);

        CheckpointState.WindowState window = restored.windowStates().getFirst();
        assertThat(window.windowStart()).isEqualTo(Instant.parse("2026-05-11T14:04:00Z"));
        assertThat(window.windowEnd()).isEqualTo(Instant.parse("2026-05-11T14:05:00Z"));
        assertThat(window.events()).hasSize(2);
        assertThat(window.events().get(0).price()).isEqualByComparingTo("67500.00");
    }

    @Test
    void serializeDeserialize_preservesMultiplePartitionOffsets() throws IOException {
        CheckpointState original = buildCheckpoint();

        byte[] bytes = CheckpointManager.serialize(original);
        CheckpointState restored = CheckpointManager.deserialize(bytes);

        assertThat(restored.consumerOffsets()).containsEntry(0, 1500L);
        assertThat(restored.consumerOffsets()).containsEntry(1, 1200L);
    }

    @Test
    void checkpointState_defensiveCopies() {
        CheckpointState state = buildCheckpoint();

        // Verify immutability
        assertThatThrownBy(() -> state.windowStates().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> state.consumerOffsets().put(99, 9999L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void checkpointState_nullFeatureName_throws() {
        assertThatThrownBy(() -> new CheckpointState(
                null, "v1", WATERMARK, List.of(), Map.of(), Instant.now()
        )).isInstanceOf(IllegalArgumentException.class);
    }

    private CheckpointState buildCheckpoint() {
        UUID id1 = UUID.fromString("01900000-0000-7000-8000-000000000001");
        UUID id2 = UUID.fromString("01900000-0000-7000-8000-000000000002");

        var trade1 = new io.muninn.shared.event.TradeEvent(
                id1,
                Instant.parse("2026-05-11T14:04:10Z"),
                Instant.parse("2026-05-11T14:04:10Z"),
                "binance",
                new io.muninn.shared.instrument.Instrument("BTC-USDT", "BTC", "USDT",
                        new io.muninn.shared.instrument.Exchange("binance", "Binance Spot", java.time.ZoneId.of("UTC"))),
                1L, 1, new java.math.BigDecimal("67500.00"), new java.math.BigDecimal("1.0"),
                io.muninn.shared.event.Side.BUY, "t1"
        );
        var trade2 = new io.muninn.shared.event.TradeEvent(
                id2,
                Instant.parse("2026-05-11T14:04:20Z"),
                Instant.parse("2026-05-11T14:04:20Z"),
                "binance",
                new io.muninn.shared.instrument.Instrument("BTC-USDT", "BTC", "USDT",
                        new io.muninn.shared.instrument.Exchange("binance", "Binance Spot", java.time.ZoneId.of("UTC"))),
                2L, 1, new java.math.BigDecimal("67510.00"), new java.math.BigDecimal("2.0"),
                io.muninn.shared.event.Side.BUY, "t2"
        );

        var window1 = new CheckpointState.WindowState(
                Instant.parse("2026-05-11T14:04:00Z"),
                Instant.parse("2026-05-11T14:05:00Z"),
                List.of(trade1, trade2)
        );

        var window2 = new CheckpointState.WindowState(
                Instant.parse("2026-05-11T14:05:00Z"),
                Instant.parse("2026-05-11T14:06:00Z"),
                List.of(trade1)
        );

        return new CheckpointState(
                "vwap.1m",
                "v1",
                WATERMARK,
                List.of(window1, window2),
                Map.of(0, 1500L, 1, 1200L),
                Instant.now()
        );
    }
}
