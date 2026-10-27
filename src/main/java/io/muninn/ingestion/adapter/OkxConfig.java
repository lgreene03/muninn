package io.muninn.ingestion.adapter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Configuration for the OKX public market-data adapter, bound from
 * {@code muninn.ingestion.okx.*}.
 *
 * <p>Public data only: no API key, no account, no trading endpoint.
 *
 * <p>Channels default to {@code trades} and {@code books5}. {@code books5} is a full
 * five-level snapshot every 100ms, the same shape as the Binance {@code depth20}
 * stream muninn already handles, so it needs no delta or resync machinery and no gap
 * detection. OKX's deeper {@code books} channel is snapshot-then-deltas and is out of
 * scope until muninn has a delta-book engine.
 *
 * @param enabled     whether the adapter runs
 * @param url         public WebSocket endpoint
 * @param instruments OKX instrument ids, e.g. {@code BTC-USDT}. Spot only.
 * @param channels    OKX channels to subscribe to for every instrument
 * @param reconnect   reconnect backoff
 */
@ConfigurationProperties(prefix = "muninn.ingestion.okx")
public record OkxConfig(
        boolean enabled,
        String url,
        List<String> instruments,
        List<String> channels,
        BinanceConfig.Reconnect reconnect
) {
    public OkxConfig {
        if (url == null || url.isBlank()) url = "wss://ws.okx.com:8443/ws/v5/public";
        if (instruments == null || instruments.isEmpty()) instruments = List.of("BTC-USDT");
        if (channels == null || channels.isEmpty()) channels = List.of("trades", "books5");
        if (reconnect == null) reconnect = new BinanceConfig.Reconnect(1000L, 60000L, true);
        instruments = List.copyOf(instruments);
        channels = List.copyOf(channels);
    }
}
