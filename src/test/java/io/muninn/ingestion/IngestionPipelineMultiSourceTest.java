package io.muninn.ingestion;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.muninn.ingestion.adapter.ExchangeAdapter;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;
import io.muninn.shared.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Multi-source ingestion pipeline tests.
 *
 * <p>Verifies the Phase 8 multi-exchange framework (ADR-0008):
 * {@link IngestionPipeline} starts every wired adapter, routes their events
 * through the shared validator, and emits per-source metric tags.</p>
 *
 * <p>Uses hand-rolled adapter test doubles and a minimal producer test double
 * — keeps the test framework-free (no Mockito) so it runs on any JDK.</p>
 */
class IngestionPipelineMultiSourceTest {

    @Test
    void onApplicationReady_startsEveryWiredAdapter() {
        RecordingAdapter binance = new RecordingAdapter("binance.spot.v1");
        RecordingAdapter coinbase = new RecordingAdapter("coinbase.pro.v1");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        IngestionPipeline pipeline = pipelineWith(registry, List.of(binance, coinbase));

        pipeline.onApplicationReady();

        assertThat(binance.started.get()).isTrue();
        assertThat(coinbase.started.get()).isTrue();
        assertThat(pipeline.activeAdapterCount()).isEqualTo(2);
    }

    @Test
    void emptyAdapterList_isIdleAndDoesNotThrow() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IngestionPipeline pipeline = pipelineWith(registry, List.of());

        pipeline.onApplicationReady();

        assertThat(pipeline.activeAdapterCount()).isZero();
    }

    @Test
    void metrics_tagAccordingToEventSource() {
        RecordingAdapter binance = new RecordingAdapter("binance.spot.v1");
        RecordingAdapter coinbase = new RecordingAdapter("coinbase.pro.v1");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IngestionPipeline pipeline = pipelineWith(registry, List.of(binance, coinbase));

        pipeline.onApplicationReady();

        // Each adapter delivers one valid trade.
        binance.deliver(tradeFor("binance.spot.v1"));
        coinbase.deliver(tradeFor("coinbase.pro.v1"));

        // Per-source counters increment independently.
        double binanceIngested = registry.find("muninn.ingest.events.total")
                .tag("source", "binance.spot.v1").counter().count();
        double coinbaseIngested = registry.find("muninn.ingest.events.total")
                .tag("source", "coinbase.pro.v1").counter().count();
        assertThat(binanceIngested).isEqualTo(1.0);
        assertThat(coinbaseIngested).isEqualTo(1.0);
    }

    @Test
    void invalidEvent_routesToDeadLetterUnderItsSourceTag() {
        RecordingAdapter binance = new RecordingAdapter("binance.spot.v1");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecordingDeadLetter dlq = new RecordingDeadLetter();
        IngestionPipeline pipeline = pipelineWith(registry, List.of(binance),
                new RecordingProducer(), dlq);

        pipeline.onApplicationReady();

        // Event from the future — the validator rejects on time-skew.
        Instant farFuture = Instant.now().plusSeconds(60 * 60 * 24 * 365);
        binance.deliver(tradeFor("binance.spot.v1", farFuture));

        assertThat(dlq.rejections).hasSize(1);
        assertThat(dlq.rejections.get(0).source).isEqualTo("binance.spot.v1");

        double rejected = registry.find("muninn.ingest.validation.failed")
                .tag("source", "binance.spot.v1").counter().count();
        assertThat(rejected).isEqualTo(1.0);
    }

    @Test
    void shutdown_stopsEveryAdapterEvenWhenOneThrows() {
        RecordingAdapter healthy = new RecordingAdapter("binance.spot.v1");
        ThrowingAdapter angry = new ThrowingAdapter("coinbase.pro.v1");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        IngestionPipeline pipeline = pipelineWith(registry, List.of(healthy, angry));

        pipeline.onApplicationReady();
        pipeline.shutdown();

        assertThat(healthy.stopped.get()).isTrue();
        assertThat(angry.stopAttempts.get()).isEqualTo(1);
    }

    // ----- helpers ----------------------------------------------------------

    private static IngestionPipeline pipelineWith(MeterRegistry registry,
                                                   List<ExchangeAdapter> adapters) {
        return pipelineWith(registry, adapters,
                new RecordingProducer(), new RecordingDeadLetter());
    }

    private static IngestionPipeline pipelineWith(MeterRegistry registry,
                                                   List<ExchangeAdapter> adapters,
                                                   RecordingProducer producer,
                                                   RecordingDeadLetter dlq) {
        MarketEventProducer mep = new MarketEventProducer(null) {
            @Override
            public java.util.concurrent.CompletableFuture<Void> publish(MarketEvent event) {
                producer.published.add(event);
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }
        };
        DeadLetterProducer dlp = new DeadLetterProducer(null) {
            @Override
            public void reject(Object rawPayload, String reason, String source) {
                dlq.rejections.add(new Rejection(rawPayload, reason, source));
            }
        };
        return new IngestionPipeline(adapters, mep, dlp, registry);
    }

    private static TradeEvent tradeFor(String source) {
        return tradeFor(source, Instant.now());
    }

    private static TradeEvent tradeFor(String source, Instant eventTime) {
        Exchange exchange = new Exchange(
                source.split("\\.")[0],
                "Test exchange for " + source,
                ZoneId.of("UTC"));
        Instrument instrument = new Instrument("BTC-USDT", "BTC", "USDT", exchange);
        return new TradeEvent(
                UUIDv7.generate(),
                eventTime,
                Instant.now(),
                source,
                instrument,
                1L,
                1,
                new BigDecimal("60000.00"),
                new BigDecimal("0.10"),
                Side.BUY,
                "t-test"
        );
    }

    /** Hand-rolled adapter that records lifecycle and lets the test deliver events. */
    private static final class RecordingAdapter implements ExchangeAdapter {
        private final String source;
        final java.util.concurrent.atomic.AtomicBoolean started = new java.util.concurrent.atomic.AtomicBoolean(false);
        final java.util.concurrent.atomic.AtomicBoolean stopped = new java.util.concurrent.atomic.AtomicBoolean(false);
        private volatile Consumer<MarketEvent> handler;

        RecordingAdapter(String source) { this.source = source; }
        @Override public String source() { return source; }
        @Override public void start(Consumer<MarketEvent> eventHandler) {
            this.handler = eventHandler;
            started.set(true);
        }
        @Override public void stop() { stopped.set(true); }
        void deliver(MarketEvent event) { handler.accept(event); }
    }

    private static final class ThrowingAdapter implements ExchangeAdapter {
        private final String source;
        final AtomicInteger stopAttempts = new AtomicInteger(0);
        ThrowingAdapter(String source) { this.source = source; }
        @Override public String source() { return source; }
        @Override public void start(Consumer<MarketEvent> eventHandler) { }
        @Override public void stop() {
            stopAttempts.incrementAndGet();
            throw new RuntimeException("stop failed");
        }
    }

    private static final class RecordingProducer {
        final List<MarketEvent> published = new ArrayList<>();
    }

    private record Rejection(Object rawPayload, String reason, String source) { }

    private static final class RecordingDeadLetter {
        final List<Rejection> rejections = new ArrayList<>();
    }

    @SuppressWarnings("unused")
    private static ValidationResult anyValid() {
        return new ValidationResult.Valid();
    }
}
