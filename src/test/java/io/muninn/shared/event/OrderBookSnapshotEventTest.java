package io.muninn.shared.event;

import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link OrderBookSnapshotEvent}.
 */
class OrderBookSnapshotEventTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    @Test
    void construct_validSnapshot_succeeds() {
        var bids = List.of(
                new PriceLevel(new BigDecimal("67500"), new BigDecimal("1.5")),
                new PriceLevel(new BigDecimal("67499"), new BigDecimal("2.0"))
        );
        var asks = List.of(
                new PriceLevel(new BigDecimal("67501"), new BigDecimal("0.5")),
                new PriceLevel(new BigDecimal("67502"), new BigDecimal("1.0"))
        );

        var snapshot = new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(),
                "binance.spot.v1", BTC_USDT, 1L, 1, bids, asks, 20
        );

        assertThat(snapshot.bids()).hasSize(2);
        assertThat(snapshot.asks()).hasSize(2);
        assertThat(snapshot.depth()).isEqualTo(20);
        assertThat(snapshot.topicName()).isEqualTo("events.book.snapshot");
    }

    @Test
    void bidsAndAsks_areDefensivelyCopied() {
        var mutableBids = new ArrayList<>(List.of(
                new PriceLevel(new BigDecimal("67500"), new BigDecimal("1.5"))
        ));
        var mutableAsks = new ArrayList<>(List.of(
                new PriceLevel(new BigDecimal("67501"), new BigDecimal("0.5"))
        ));

        var snapshot = new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(),
                "src", BTC_USDT, 1L, 1, mutableBids, mutableAsks, 1
        );

        // Modifying the original list must not affect the snapshot
        mutableBids.add(new PriceLevel(new BigDecimal("67499"), BigDecimal.ONE));
        assertThat(snapshot.bids()).hasSize(1);

        // The stored list must be unmodifiable
        assertThatThrownBy(() -> snapshot.bids().add(
                new PriceLevel(BigDecimal.ONE, BigDecimal.ONE)
        )).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void construct_zeroDepth_throwsIllegalArgument() {
        assertThatThrownBy(() -> new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(),
                "src", BTC_USDT, 1L, 1,
                List.of(new PriceLevel(BigDecimal.ONE, BigDecimal.ONE)),
                List.of(new PriceLevel(BigDecimal.ONE, BigDecimal.ONE)),
                0
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("depth must be positive");
    }

    @Test
    void construct_nullBids_throwsIllegalArgument() {
        assertThatThrownBy(() -> new OrderBookSnapshotEvent(
                UUIDv7.generate(), Instant.now(), Instant.now(),
                "src", BTC_USDT, 1L, 1, null,
                List.of(new PriceLevel(BigDecimal.ONE, BigDecimal.ONE)), 1
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bids is required");
    }
}
