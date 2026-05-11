package io.muninn.ingestion.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure-function parser for Binance depth (order book) WebSocket messages.
 *
 * <p>Binance partial book depth stream format (depth20@100ms):
 * <pre>{@code
 * {
 *   "lastUpdateId": 160,
 *   "bids": [["0.0024","10"], ...],
 *   "asks": [["0.0026","100"], ...]
 * }
 * }</pre>
 *
 * <p>This class has no side effects and is safe for deterministic testing.</p>
 */
public final class BinanceBookParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BinanceBookParser() {
        // Utility class
    }

    /**
     * Parse a Binance partial book depth WebSocket message into a canonical
     * {@link OrderBookSnapshotEvent}.
     *
     * @param json           the raw JSON message from Binance
     * @param instrument     the canonical instrument
     * @param source         the adapter source identifier
     * @param ingestTime     when Muninn observed this message
     * @param sequenceNumber monotonic sequence number
     * @return the parsed order book snapshot event
     * @throws BinanceParseException if the message cannot be parsed
     */
    public static OrderBookSnapshotEvent parse(String json, Instrument instrument, String source,
                                               Instant ingestTime, long sequenceNumber) {
        try {
            JsonNode node = MAPPER.readTree(json);

            List<PriceLevel> bids = parseLevels(node.get("bids"));
            List<PriceLevel> asks = parseLevels(node.get("asks"));

            int depth = Math.max(bids.size(), asks.size());

            return new OrderBookSnapshotEvent(
                    UUIDv7.generate(ingestTime.toEpochMilli()),
                    ingestTime, // Binance depth snapshots don't carry an event timestamp
                    ingestTime,
                    source,
                    instrument,
                    sequenceNumber,
                    OrderBookSnapshotEvent.CURRENT_SCHEMA_VERSION,
                    bids,
                    asks,
                    depth
            );
        } catch (Exception e) {
            throw new BinanceParseException("Failed to parse Binance book snapshot", json, e);
        }
    }

    private static List<PriceLevel> parseLevels(JsonNode levelsNode) {
        List<PriceLevel> levels = new ArrayList<>();
        if (levelsNode != null && levelsNode.isArray()) {
            for (JsonNode level : levelsNode) {
                BigDecimal price = new BigDecimal(level.get(0).asText());
                BigDecimal size = new BigDecimal(level.get(1).asText());
                if (size.signum() > 0) { // Skip zero-size levels
                    levels.add(new PriceLevel(price, size));
                }
            }
        }
        return levels;
    }
}
