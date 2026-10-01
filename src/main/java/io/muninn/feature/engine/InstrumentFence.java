package io.muninn.feature.engine;

import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.instrument.Instrument;

/**
 * Admits only the one instrument, on the one venue, that the feature engine computes.
 *
 * <p>The engine's windows are keyed by start time alone, and its computers fold every
 * event in a window together: VWAP sums all trades, OBI and micro-price take whichever
 * book snapshot arrived last, VPIN folds a batch into its first trade's instrument, and
 * every output is published under one instrument key. That is correct while exactly one
 * instrument flows. The moment a second venue or symbol is ingested it is silently
 * wrong: USDT and USD prices average into one VWAP, OBI flips between two books.
 *
 * <p>So the engine is fenced to one {@code (exchange id, symbol)} before any second
 * venue exists. Making features venue-aware is the real fix, and a breaking change to
 * every consumer of the feature topics; the fence makes adding venues safe until then.
 *
 * <p>A fenced-out event touches nothing in the engine: not the windows, not the
 * watermark, not the recorded offsets. Venues share a partition (the Kafka key is the
 * symbol) and their clocks and latencies differ, so letting another venue advance the
 * watermark would get this venue's events dropped as late near every window boundary.
 * And checkpoint restore seeds a watermark for every partition in the recorded offsets,
 * so recording a partition that carries only other venues would let it hold the global
 * minimum back for ever. With the symbol as key, every admitted event lands in one
 * partition, so the admitted stream's watermark is exactly what it was before the fence.
 *
 * @param exchangeId the venue, matching {@code instrument.exchange.id} (e.g. {@code binance})
 * @param symbol     the canonical symbol, matching {@code instrument.symbol} (e.g. {@code BTC-USDT})
 */
public record InstrumentFence(String exchangeId, String symbol) {

    public InstrumentFence {
        if (exchangeId == null || exchangeId.isBlank()) throw new IllegalArgumentException("exchangeId is required");
        if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("symbol is required");
    }

    public boolean admits(MarketEvent event) {
        Instrument instrument = event.instrument();
        return instrument != null
                && symbol.equals(instrument.symbol())
                && instrument.exchange() != null
                && exchangeId.equals(instrument.exchange().id());
    }
}
