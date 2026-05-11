package io.muninn.shared.instrument;

/**
 * A trade-able instrument on an exchange.
 *
 * <p>The {@code symbol} uses exchange-agnostic naming (e.g., {@code "BTC-USD"})
 * regardless of the underlying exchange's native symbol format.</p>
 *
 * @param symbol     canonical symbol (e.g., {@code "BTC-USD"})
 * @param baseAsset  the base asset (e.g., {@code "BTC"})
 * @param quoteAsset the quote asset (e.g., {@code "USD"} or {@code "USDT"})
 * @param exchange   the exchange this instrument trades on
 */
public record Instrument(
        String symbol,
        String baseAsset,
        String quoteAsset,
        Exchange exchange
) implements java.io.Serializable {

    public Instrument {
        if (symbol == null || symbol.isBlank()) throw new IllegalArgumentException("Instrument symbol is required");
        if (baseAsset == null || baseAsset.isBlank()) throw new IllegalArgumentException("Instrument baseAsset is required");
        if (quoteAsset == null || quoteAsset.isBlank()) throw new IllegalArgumentException("Instrument quoteAsset is required");
        if (exchange == null) throw new IllegalArgumentException("Instrument exchange is required");
    }
}
