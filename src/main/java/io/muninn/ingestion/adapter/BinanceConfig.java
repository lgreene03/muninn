package io.muninn.ingestion.adapter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Configuration for the Binance WebSocket adapter.
 *
 * <p>Bound via {@code @ConfigurationProperties} — no {@code @Value} injection.
 * Defaults are suitable for local development against Binance's public testnet.</p>
 */
@ConfigurationProperties(prefix = "muninn.ingestion.binance")
public record BinanceConfig(
        boolean enabled,
        String baseUrl,
        List<String> instruments,
        List<String> streams,
        Reconnect reconnect
) {

    public BinanceConfig {
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = "wss://stream.binance.com:9443";
        }
        if (instruments == null || instruments.isEmpty()) {
            instruments = List.of("btcusdt");
        }
        if (streams == null || streams.isEmpty()) {
            streams = List.of("trade", "depth20@100ms");
        }
        if (reconnect == null) {
            reconnect = new Reconnect(1000L, 60000L, true);
        }
    }

    public record Reconnect(
            long initialDelayMs,
            long maxDelayMs,
            boolean jitter
    ) {
        public Reconnect {
            if (initialDelayMs <= 0) initialDelayMs = 1000L;
            if (maxDelayMs <= 0) maxDelayMs = 60000L;
        }
    }
}
