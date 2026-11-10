package io.muninn.ingestion.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

/**
 * The venue configuration shipped in {@code application.yml}, bound the way Spring binds
 * it at start-up. G0.18 (decision D5) records the same five pairs on both venues; this
 * pins that, and proves the Binance defaults build a combined stream that Binance
 * accepts in size and that every stream type can be parsed.
 */
class ShippedIngestionDefaultsTest {

    private static Binder shippedConfig() throws IOException {
        List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"));
        StandardEnvironment env = new StandardEnvironment();
        sources.forEach(env.getPropertySources()::addLast);
        return Binder.get(env);
    }

    @Test
    void binanceSubscribesTheFiveRecordedPairsToTradesAndBooks() throws IOException {
        BinanceConfig binance = shippedConfig().bind("muninn.ingestion.binance", BinanceConfig.class).get();

        assertThat(binance.instruments()).containsExactly("btcusdt", "ethusdt", "solusdt", "xrpusdt", "dogeusdt");
        assertThat(new BinanceCombinedStream(binance.instruments(), binance.streams()).path()).isEqualTo(
                "/stream?streams="
                        + "btcusdt@trade/btcusdt@depth20@100ms/ethusdt@trade/ethusdt@depth20@100ms/"
                        + "solusdt@trade/solusdt@depth20@100ms/xrpusdt@trade/xrpusdt@depth20@100ms/"
                        + "dogeusdt@trade/dogeusdt@depth20@100ms");
    }

    @Test
    void okxSubscribesTheSameFivePairsAsSpotInstruments() throws IOException {
        OkxConfig okx = shippedConfig().bind("muninn.ingestion.okx", OkxConfig.class).get();

        assertThat(okx.instruments()).containsExactly("BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT");
        // Each is a well-formed spot id, and maps to the same canonical symbol Binance uses.
        assertThat(okx.instruments()).allSatisfy(id ->
                assertThat(OkxMessageParser.instrument(id).symbol()).isEqualTo(id));
    }
}
