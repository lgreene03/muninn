package io.muninn.feature.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.feature.checkpoint.CheckpointManager;
import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The feature engine computes one instrument on one venue, and a second venue on the
 * same topics must neither corrupt its figures nor stop its windows firing.
 */
class FeatureEngineFenceTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Exchange COINBASE = new Exchange("coinbase", "Coinbase", ZoneId.of("UTC"));
    private static final Exchange OKX = new Exchange("okx", "OKX", ZoneId.of("UTC"));
    private static final Instrument BINANCE_BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final Instrument COINBASE_BTC_USD = new Instrument("BTC-USD", "BTC", "USD", COINBASE);
    // The collision the ingestion map warned about: same symbol, different venue.
    private static final Instrument COINBASE_BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", COINBASE);
    private static final Instrument OKX_BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", OKX);

    private static long seq;

    private static TradeEvent trade(Instrument instrument, String time, String price) {
        Instant t = Instant.parse(time);
        return new TradeEvent(UUIDv7.generate(), t, t, instrument.exchange().id() + ".spot.v1", instrument,
                ++seq, 1, new BigDecimal(price), new BigDecimal("1"), Side.BUY, "t-" + seq);
    }

    private static FeatureEngineConfig config(String exchange, String symbol) {
        return new FeatureEngineConfig(true, Duration.ofMinutes(1), Duration.ofMinutes(5), "fence-test", "drop",
                10, new BigDecimal("100"), 50, exchange, symbol);
    }

    record Run(List<Published> published, FeatureEngineRunner runner) {}

    private static List<Published> run(FeatureEngineConfig config, List<EventSource.PartitionedEvent> script) {
        return runFully(config, script).published();
    }

    /** Runs the real runner over a script; returns what it published and the runner. */
    @SuppressWarnings("unchecked")
    private static Run runFully(FeatureEngineConfig config, List<EventSource.PartitionedEvent> script) {
        KafkaTemplate<String, FeatureComputedEvent> kafka = mock(KafkaTemplate.class);
        FeatureEngineRunner runner = new FeatureEngineRunner(
                new Scripted(script),
                new WindowManager(config.windowDuration(), new WatermarkTracker()),
                config, kafka,
                new CheckpointManager(mock(S3Client.class), new SimpleMeterRegistry()),
                new SimpleMeterRegistry());
        runner.run();

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<FeatureComputedEvent> events = ArgumentCaptor.forClass(FeatureComputedEvent.class);
        try {
            verify(kafka, atLeastOnce()).send(anyString(), keys.capture(), events.capture());
        } catch (AssertionError none) {
            return new Run(List.of(), runner);
        }
        List<Published> out = new java.util.ArrayList<>();
        for (int i = 0; i < events.getAllValues().size(); i++) {
            out.add(new Published(keys.getAllValues().get(i), events.getAllValues().get(i)));
        }
        return new Run(out, runner);
    }

    record Published(String key, FeatureComputedEvent event) {}

    private static EventSource.PartitionedEvent at(MarketEvent e, int partition, long offset) {
        return new EventSource.PartitionedEvent(e, partition, offset);
    }

    private static List<BigDecimal> vwaps(List<Published> published) {
        return published.stream().map(Published::event)
                .filter(e -> e.featureName().equals("vwap.1m"))
                .map(FeatureComputedEvent::value).toList();
    }

    @Test
    void aSecondVenueIsNotMixedIntoTheFencedInstrumentsVwap() {
        // Binance at 100 and Coinbase at 200 in the same minute. Unfenced, VWAP is 150:
        // a price that no venue ever quoted, published as Binance's.
        List<Published> published = run(config("binance", "BTC-USDT"), List.of(
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:10Z", "100"), 0, 0),
                at(trade(COINBASE_BTC_USD, "2026-10-01T14:00:20Z", "200"), 0, 1),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:01:10Z", "100"), 0, 2)));

        assertThat(vwaps(published)).isNotEmpty()
                .allSatisfy(v -> assertThat(v).isEqualByComparingTo("100"));
    }

    @Test
    void theSameSymbolOnAnotherVenueIsAlsoFencedOut() {
        // Symbol alone is not identity. Coinbase BTC-USDT must not leak into Binance BTC-USDT.
        List<Published> published = run(config("binance", "BTC-USDT"), List.of(
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:10Z", "100"), 0, 0),
                at(trade(COINBASE_BTC_USDT, "2026-10-01T14:00:20Z", "200"), 0, 1),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:01:10Z", "100"), 0, 2)));

        assertThat(vwaps(published)).isNotEmpty()
                .allSatisfy(v -> assertThat(v).isEqualByComparingTo("100"));
    }

    @Test
    void anotherVenueInTheSamePartitionCannotMakeThisVenueLate() {
        // The production case. The Kafka key is the symbol, so Binance and OKX BTC-USDT
        // share a partition, and venue latencies differ. Here an OKX event from 14:01:00.05
        // arrives just before a Binance event from 14:00:59.90. If the OKX event advanced
        // the partition's watermark, the Binance event's window (ending 14:01:00) would
        // already be closed and it would be DROPPED AS LATE: real Binance data silently
        // lost near every window boundary once a second venue is switched on.
        //
        // Binance's two trades in the 14:00 window are 100 and 110, so VWAP is 105 only
        // if the later one survives, and 100 if it was dropped. An earlier version of the
        // fence advanced the watermark for fenced-out events and would give 100.
        List<Published> published = run(config("binance", "BTC-USDT"), List.of(
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:10.000Z", "100"), 0, 0),
                at(trade(OKX_BTC_USDT, "2026-10-01T14:01:00.050Z", "999"), 0, 1),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:59.900Z", "110"), 0, 2),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:01:10.000Z", "100"), 0, 3)));

        assertThat(vwaps(published)).as("the 14:00 window's VWAP").isNotEmpty();
        assertThat(vwaps(published).getFirst())
                .as("both Binance trades in the minute are kept, so (100 + 110) / 2")
                .isEqualByComparingTo("105");
    }

    @Test
    void aPartitionCarryingOnlyAnotherVenueNeverHoldsWindowsBack() {
        // Windows fire on the minimum watermark across registered partitions. Partition 1
        // carries only OKX from the start, so it must never be registered at all.
        List<Published> published = run(config("binance", "BTC-USDT"), List.of(
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:10Z", "100"), 0, 0),
                at(trade(OKX_BTC_USDT, "2026-10-01T14:00:05Z", "999"), 1, 0),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:01:10Z", "100"), 0, 1),
                at(trade(OKX_BTC_USDT, "2026-10-01T14:00:06Z", "999"), 1, 1),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:02:10Z", "100"), 0, 2)));

        assertThat(vwaps(published)).as("windows closed despite partition 1's older OKX times")
                .hasSizeGreaterThanOrEqualTo(2)
                .allSatisfy(v -> assertThat(v).isEqualByComparingTo("100"));
    }

    @Test
    void fencedOutPartitionsAreNeverRecordedForCheckpointRestore() {
        // Checkpoint restore seeds a watermark for every partition in the recorded offsets.
        // A partition carrying only another venue must therefore never be recorded, or a
        // restart would register it and it would hold the global minimum back for ever.
        Run run = runFully(config("binance", "BTC-USDT"), List.of(
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:10Z", "100"), 0, 0),
                at(trade(OKX_BTC_USDT, "2026-10-01T14:00:20Z", "999"), 1, 7),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:01:10Z", "100"), 0, 1)));

        assertThat(run.runner().recordedOffsets()).containsOnlyKeys(0).containsEntry(0, 1L);
    }

    @Test
    void outputsArePublishedUnderTheFencedSymbolNotAConstant() {
        // Fence to Coinbase BTC-USD: the label must follow the configuration. It used to
        // be a hardcoded "BTC-USDT" whatever was actually windowed.
        List<Published> published = run(config("coinbase", "BTC-USD"), List.of(
                at(trade(COINBASE_BTC_USD, "2026-10-01T14:00:10Z", "200"), 0, 0),
                at(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:20Z", "100"), 0, 1),
                at(trade(COINBASE_BTC_USD, "2026-10-01T14:01:10Z", "200"), 0, 2)));

        assertThat(published).isNotEmpty().allSatisfy(p -> assertThat(p.key()).isEqualTo("BTC-USD"));
        assertThat(vwaps(published)).allSatisfy(v -> assertThat(v).isEqualByComparingTo("200"));
    }

    @Test
    void theDefaultFenceIsWhatTheEngineHasAlwaysComputed() {
        // So enabling the fence changes nothing until a second venue is ingested.
        InstrumentFence fence = new FeatureEngineConfig(true, null, null, null, null, null, null, null, null, null).fence();
        assertThat(fence).isEqualTo(new InstrumentFence("binance", "BTC-USDT"));
    }

    @Test
    void fenceMatchesVenueAndSymbolTogether() {
        InstrumentFence fence = new InstrumentFence("binance", "BTC-USDT");
        assertThat(fence.admits(trade(BINANCE_BTC_USDT, "2026-10-01T14:00:00Z", "1"))).isTrue();
        assertThat(fence.admits(trade(COINBASE_BTC_USDT, "2026-10-01T14:00:00Z", "1"))).isFalse();
        assertThat(fence.admits(trade(COINBASE_BTC_USD, "2026-10-01T14:00:00Z", "1"))).isFalse();
    }

    private static final class Scripted implements EventSource {
        private final Deque<PartitionedEvent> queue;

        Scripted(List<PartitionedEvent> events) {
            this.queue = new ArrayDeque<>(events);
        }

        @Override
        public Optional<PartitionedEvent> poll() {
            return Optional.ofNullable(queue.poll());
        }

        @Override
        public boolean hasMore() {
            return !queue.isEmpty();
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    }
}
