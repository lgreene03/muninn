package io.muninn.ingestion.adapter;

import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link BinanceTradeParser}.
 * Uses recorded Binance payloads from src/test/resources/fixtures/binance/.
 */
class BinanceTradeParserTest {

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final Instrument BTC_USDT = new Instrument("BTC-USDT", "BTC", "USDT", BINANCE);
    private static final String SOURCE = "binance.spot.v1";

    @Test
    void parse_validTradeMessage_producesCorrectEvent() throws IOException {
        String json = loadFixture("fixtures/binance/trade.json");
        Instant ingestTime = Instant.parse("2026-05-11T12:00:00.500Z");

        TradeEvent trade = BinanceTradeParser.parse(json, BTC_USDT, SOURCE, ingestTime, 42L);

        assertThat(trade.eventId()).isNotNull();
        assertThat(trade.eventTime()).isEqualTo(Instant.ofEpochMilli(1715000000000L));
        assertThat(trade.ingestTime()).isEqualTo(ingestTime);
        assertThat(trade.source()).isEqualTo(SOURCE);
        assertThat(trade.instrument()).isEqualTo(BTC_USDT);
        assertThat(trade.sequenceNumber()).isEqualTo(42L);
        assertThat(trade.schemaVersion()).isEqualTo(1);
        assertThat(trade.price()).isEqualByComparingTo("67523.45");
        assertThat(trade.size()).isEqualByComparingTo("0.00150");
        assertThat(trade.side()).isEqualTo(Side.BUY); // m=false → buyer is taker → BUY
        assertThat(trade.exchangeTradeId()).isEqualTo("4200001");
    }

    @Test
    void parse_buyerIsMaker_sideIsSell() throws IOException {
        // When "m"=true, the buyer is the maker, so the trade was initiated by a seller
        String json = """
                {"e":"trade","E":1715000000000,"s":"BTCUSDT","t":100,"p":"67500.00","q":"1.0","T":1715000000000,"m":true,"M":true}
                """;

        TradeEvent trade = BinanceTradeParser.parse(json, BTC_USDT, SOURCE, Instant.now(), 1L);
        assertThat(trade.side()).isEqualTo(Side.SELL);
    }

    @Test
    void parse_invalidJson_throwsBinanceParseException() {
        assertThatThrownBy(() -> BinanceTradeParser.parse(
                "not valid json", BTC_USDT, SOURCE, Instant.now(), 1L
        )).isInstanceOf(BinanceParseException.class)
                .hasMessageContaining("Failed to parse Binance trade message");
    }

    @Test
    void parse_bigDecimalPrecisionPreserved() throws IOException {
        String json = """
                {"e":"trade","E":1715000000000,"s":"BTCUSDT","t":100,"p":"67523.12345678","q":"0.00000001","T":1715000000000,"m":false,"M":true}
                """;

        TradeEvent trade = BinanceTradeParser.parse(json, BTC_USDT, SOURCE, Instant.now(), 1L);
        assertThat(trade.price()).isEqualByComparingTo("67523.12345678");
        assertThat(trade.size()).isEqualByComparingTo("0.00000001");
    }

    private String loadFixture(String path) throws IOException {
        return new String(getClass().getClassLoader().getResourceAsStream(path).readAllBytes());
    }
}
