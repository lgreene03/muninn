package io.muninn.ingestion.adapter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Configuration for the Coinbase Pro adapter.
 *
 * <p>Mirrors {@link BinanceConfig}'s shape so adding a third exchange is a
 * one-record exercise. The Coinbase adapter is the reference second source for
 * the multi-exchange ingestion framework documented in ADR-0008.</p>
 */
@ConfigurationProperties(prefix = "muninn.ingestion.coinbase")
public record CoinbaseConfig(
        boolean enabled,
        String baseUrl,
        List<String> instruments,
        List<String> channels
) {

    public CoinbaseConfig {
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = "wss://ws-feed.exchange.coinbase.com";
        }
        if (instruments == null || instruments.isEmpty()) {
            instruments = List.of("BTC-USD");
        }
        if (channels == null || channels.isEmpty()) {
            channels = List.of("matches", "level2");
        }
    }
}
