package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for the same cross-topic ordering defect covered by
 * {@code LiveEventSourceTest}, at the replay path. Additionally proves the more
 * severe variant this class had: without sorting first, an out-of-range record
 * encountered early (in unsorted, per-partition fetch order) triggered the "past
 * toTime, stop" break BEFORE an in-range record from a different partition in the
 * very same batch was ever added to the buffer — silently dropping it, not just
 * reordering it.
 */
class ReplayEventSourceTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instant FROM = Instant.parse("2026-05-11T14:00:00Z");
    private static final Instant TO = Instant.parse("2026-05-11T14:02:00Z");

    @SuppressWarnings("unchecked")
    @Test
    void poll_batchSpanningTwoTopics_deliversInRangeEventsFromBothPartitions_inEventTimeOrder() {
        // In range, on the "trade" partition together with an OUT-OF-RANGE trade.
        TradeEvent inRangeTrade = trade(Instant.parse("2026-05-11T14:00:05Z"), 1L);
        TradeEvent outOfRangeTrade = trade(Instant.parse("2026-05-11T14:05:00Z"), 2L); // >= TO

        // In range, on the "snapshot" partition, event-time BETWEEN the two trades above.
        OrderBookSnapshotEvent inRangeSnapshot = snapshot(Instant.parse("2026-05-11T14:00:40Z"));

        // Adversarial fetch order: the trade partition (containing the out-of-range
        // record) is iterated BEFORE the snapshot partition. Unsorted, the old code
        // would hit outOfRangeTrade, set running=false and break — WITHOUT EVER
        // LOOKING AT inRangeSnapshot, dropping a genuinely in-range event.
        Map<TopicPartition, List<ConsumerRecord<String, MarketEvent>>> perPartition = new LinkedHashMap<>();
        perPartition.put(new TopicPartition("events.trade", 0), List.of(
                new ConsumerRecord<>("events.trade", 0, 0L, "BTC-USDT", (MarketEvent) inRangeTrade),
                new ConsumerRecord<>("events.trade", 0, 1L, "BTC-USDT", (MarketEvent) outOfRangeTrade)
        ));
        perPartition.put(new TopicPartition("events.book.snapshot", 0), List.of(
                new ConsumerRecord<>("events.book.snapshot", 0, 0L, "BTC-USDT", (MarketEvent) inRangeSnapshot)
        ));
        ConsumerRecords<String, MarketEvent> records = new ConsumerRecords<>(perPartition);

        KafkaConsumer<String, MarketEvent> mockConsumer = mock(KafkaConsumer.class);
        Set<TopicPartition> assigned = Set.of(
                new TopicPartition("events.trade", 0), new TopicPartition("events.book.snapshot", 0));
        when(mockConsumer.assignment()).thenReturn(assigned);
        Map<TopicPartition, OffsetAndTimestamp> offsets = new LinkedHashMap<>();
        for (TopicPartition tp : assigned) {
            offsets.put(tp, new OffsetAndTimestamp(0L, FROM.toEpochMilli()));
        }
        when(mockConsumer.offsetsForTimes(anyMap())).thenReturn(offsets);
        when(mockConsumer.poll(any())).thenReturn(records, ConsumerRecords.empty());

        ReplayEventSource source = new ReplayEventSource(
                mockConsumer, List.of("events.trade", "events.book.snapshot"), FROM, TO);
        source.start();

        List<MarketEvent> delivered = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            source.poll().ifPresent(pe -> delivered.add(pe.event()));
        }

        assertThat(delivered)
                .as("both in-range events must be delivered, in event-time order, even though the "
                        + "out-of-range trade was iterated before the in-range snapshot in fetch order")
                .containsExactly(inRangeTrade, inRangeSnapshot);
    }

    private TradeEvent trade(Instant eventTime, long seq) {
        return new TradeEvent(
                UUIDv7.generate(), eventTime, eventTime, "test", BTC_USDT, seq, 1,
                new BigDecimal("100.00"), new BigDecimal("1.0"), Side.BUY, "t-" + seq);
    }

    private OrderBookSnapshotEvent snapshot(Instant eventTime) {
        return new OrderBookSnapshotEvent(
                UUIDv7.generate(), eventTime, eventTime, "test", BTC_USDT, 1L, 1,
                List.of(new PriceLevel(new BigDecimal("100"), new BigDecimal("1"))),
                List.of(new PriceLevel(new BigDecimal("101"), new BigDecimal("1"))),
                1);
    }
}
