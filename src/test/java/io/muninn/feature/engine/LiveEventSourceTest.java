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
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for the cross-topic ordering bug found investigating a CI failure
 * in {@code MicrostructureReplayParityIntegrationTest}: a single {@code poll()} can
 * return records from more than one subscribed topic (e.g. {@code events.trade} and
 * {@code events.book.snapshot}), and {@code ConsumerRecords} groups them by
 * {@code TopicPartition} in Kafka-client fetch order, which is unrelated to event
 * time across different topics. Without sorting, a later-produced-but-earlier-in-
 * event-time record from one topic can be delivered to {@code WindowManager} AFTER
 * an already-past-its-window record from another topic, and get dropped as late —
 * even though nothing was actually late by event time.
 */
class LiveEventSourceTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);

    @SuppressWarnings("unchecked")
    @Test
    void poll_batchSpanningTwoTopics_deliversInEventTimeOrderRegardlessOfFetchOrder() {
        TradeEvent early = trade(Instant.parse("2026-05-11T14:00:05Z"), 1L);
        OrderBookSnapshotEvent middle = snapshot(Instant.parse("2026-05-11T14:00:40Z"));
        TradeEvent late = trade(Instant.parse("2026-05-11T14:01:05Z"), 2L);

        // Deliberately construct the fetch-order map so the "trade" partition (which
        // contains BOTH the early and the late event) is iterated before the
        // "snapshot" partition — the exact adversarial ordering that caused the
        // real CI failure, reproduced here without any Kafka broker.
        Map<TopicPartition, List<ConsumerRecord<String, MarketEvent>>> perPartition = new LinkedHashMap<>();
        perPartition.put(new TopicPartition("events.trade", 0), List.of(
                new ConsumerRecord<>("events.trade", 0, 0L, "BTC-USDT", (MarketEvent) early),
                new ConsumerRecord<>("events.trade", 0, 1L, "BTC-USDT", (MarketEvent) late)
        ));
        perPartition.put(new TopicPartition("events.book.snapshot", 0), List.of(
                new ConsumerRecord<>("events.book.snapshot", 0, 0L, "BTC-USDT", (MarketEvent) middle)
        ));
        ConsumerRecords<String, MarketEvent> records = new ConsumerRecords<>(perPartition);

        KafkaConsumer<String, MarketEvent> mockConsumer = mock(KafkaConsumer.class);
        when(mockConsumer.poll(any())).thenReturn(records, ConsumerRecords.empty());

        LiveEventSource source = new LiveEventSource(mockConsumer, List.of("events.trade", "events.book.snapshot"));
        source.start();

        List<MarketEvent> delivered = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            source.poll().ifPresent(pe -> delivered.add(pe.event()));
        }

        assertThat(delivered)
                .as("events from a single poll() batch must be delivered in event-time order, "
                        + "not per-partition fetch order")
                .containsExactly(early, middle, late);
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
