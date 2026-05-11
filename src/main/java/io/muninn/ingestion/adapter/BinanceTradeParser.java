package io.muninn.ingestion.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Pure-function parser for Binance trade WebSocket messages.
 *
 * <p>Binance trade stream format (example):
 * <pre>{@code
 * {
 *   "e": "trade",
 *   "E": 1672515782136,
 *   "s": "BTCUSDT",
 *   "t": 100,
 *   "p": "0.001",
 *   "q": "100",
 *   "T": 1672515782136,
 *   "m": true,
 *   "M": true
 * }
 * }</pre>
 *
 * <p>This class has no side effects and is safe for deterministic testing.</p>
 */
public final class BinanceTradeParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BinanceTradeParser() {
        // Utility class
    }

    /**
     * Parse a Binance trade WebSocket JSON message into a canonical {@link TradeEvent}.
     *
     * @param json       the raw JSON message from Binance
     * @param instrument the canonical instrument this trade belongs to
     * @param source     the adapter source identifier
     * @param ingestTime when Muninn observed this message
     * @param sequenceNumber monotonic sequence number for this (source, instrument)
     * @return the parsed trade event
     * @throws BinanceParseException if the message cannot be parsed
     */
    public static TradeEvent parse(String json, Instrument instrument, String source,
                                   Instant ingestTime, long sequenceNumber) {
        try {
            JsonNode node = MAPPER.readTree(json);

            // Binance "T" = trade time in epoch millis
            long tradeTimeMillis = node.get("T").asLong();
            Instant eventTime = Instant.ofEpochMilli(tradeTimeMillis);

            BigDecimal price = new BigDecimal(node.get("p").asText());
            BigDecimal size = new BigDecimal(node.get("q").asText());

            // Binance "m" = true means the buyer is the maker (i.e., the trade was a sell aggressor)
            boolean isBuyerMaker = node.get("m").asBoolean();
            Side side = isBuyerMaker ? Side.SELL : Side.BUY;

            String exchangeTradeId = String.valueOf(node.get("t").asLong());

            return new TradeEvent(
                    UUIDv7.generate(tradeTimeMillis),
                    eventTime,
                    ingestTime,
                    source,
                    instrument,
                    sequenceNumber,
                    TradeEvent.CURRENT_SCHEMA_VERSION,
                    price,
                    size,
                    side,
                    exchangeTradeId
            );
        } catch (Exception e) {
            throw new BinanceParseException("Failed to parse Binance trade message", json, e);
        }
    }
}
