package io.muninn.ingestion.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.event.OrderBookSnapshotEvent;
import io.muninn.shared.event.PriceLevel;
import io.muninn.shared.event.Side;
import io.muninn.shared.event.TradeEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;
import io.muninn.shared.time.UUIDv7;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Turns one OKX public WebSocket message into canonical events. Pure: no I/O.
 *
 * <p>Behaviour pinned against real messages captured from the live feed
 * ({@code src/test/resources/okx}, refreshed by {@code scripts/probe_okx.py}):
 *
 * <ul>
 *   <li>Prices and sizes arrive as JSON strings and are read as {@link BigDecimal} from
 *       the string, so the venue's quoted precision is kept exactly.</li>
 *   <li>{@code side} on a trade is the TAKER side, so it maps straight to the
 *       aggressor. Coinbase reports the maker side and must be inverted; OKX must not
 *       be.</li>
 *   <li>{@code ts} is epoch milliseconds and is used as the event time, so book events
 *       carry exchange time rather than arrival time.</li>
 *   <li>A {@code books5} level is {@code [price, size, "0", orderCount]}. Only price
 *       and size are used.</li>
 * </ul>
 *
 * <p>Never throws on bad input: a message it cannot use comes back as a {@link Result}
 * with a reason, so the adapter can count it instead of losing a connection over it.
 */
final class OkxMessageParser {

    static final String SOURCE = "okx.spot.v1";
    static final Exchange OKX = new Exchange("okx", "OKX", ZoneId.of("UTC"));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Events produced, plus a reason when part or all of the message was unusable. */
    record Result(List<MarketEvent> events, String problem, boolean venueError) {
        static Result ok(List<MarketEvent> events) {
            return new Result(List.copyOf(events), null, false);
        }

        static Result problem(String why) {
            return new Result(List.of(), why, false);
        }
    }

    private final LongSupplier tradeSequence;
    private final LongSupplier bookSequence;

    OkxMessageParser(LongSupplier tradeSequence, LongSupplier bookSequence) {
        this.tradeSequence = tradeSequence;
        this.bookSequence = bookSequence;
    }

    Result parse(String raw, Instant ingestTime) {
        if (raw == null || raw.isBlank() || "pong".equals(raw)) {
            return Result.ok(List.of());
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(raw);
        } catch (Exception e) {
            return Result.problem("not JSON: " + e.getMessage());
        }
        if (root.has("event")) {
            String event = root.path("event").asText();
            if ("error".equals(event)) {
                return new Result(List.of(), "venue error " + root.path("code").asText() + ": "
                        + root.path("msg").asText(), true);
            }
            return Result.ok(List.of()); // subscribe acks and the like
        }
        String channel = root.path("arg").path("channel").asText();
        JsonNode data = root.path("data");
        if (!data.isArray()) {
            return Result.problem("no data array for channel " + channel);
        }

        List<MarketEvent> out = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        for (JsonNode item : data) {
            try {
                MarketEvent e = switch (channel) {
                    case "trades" -> trade(item, ingestTime);
                    case "books5" -> book(item, ingestTime);
                    default -> null;
                };
                if (e != null) {
                    out.add(e);
                } else if (!"trades".equals(channel) && !"books5".equals(channel)) {
                    problems.add("unhandled channel " + channel);
                }
            } catch (RuntimeException ex) {
                problems.add(channel + ": " + ex.getMessage());
            }
        }
        return new Result(List.copyOf(out), problems.isEmpty() ? null : String.join("; ", problems), false);
    }

    private TradeEvent trade(JsonNode t, Instant ingestTime) {
        Instrument instrument = instrument(t.path("instId").asText());
        Side side = switch (t.path("side").asText()) {
            case "buy" -> Side.BUY;   // the taker bought: buyer-initiated
            case "sell" -> Side.SELL; // the taker sold: seller-initiated
            default -> Side.UNKNOWN;
        };
        return new TradeEvent(UUIDv7.generate(), Instant.ofEpochMilli(t.path("ts").asLong()), ingestTime,
                SOURCE, instrument, tradeSequence.getAsLong(), TradeEvent.CURRENT_SCHEMA_VERSION,
                decimal(t, "px"), decimal(t, "sz"), side, t.path("tradeId").asText());
    }

    private OrderBookSnapshotEvent book(JsonNode b, Instant ingestTime) {
        Instrument instrument = instrument(b.path("instId").asText());
        List<PriceLevel> bids = levels(b.path("bids"));
        List<PriceLevel> asks = levels(b.path("asks"));
        if (bids.isEmpty() || asks.isEmpty()) {
            throw new IllegalArgumentException("book with an empty side");
        }
        return new OrderBookSnapshotEvent(UUIDv7.generate(), Instant.ofEpochMilli(b.path("ts").asLong()), ingestTime,
                SOURCE, instrument, bookSequence.getAsLong(), OrderBookSnapshotEvent.CURRENT_SCHEMA_VERSION,
                bids, asks, Math.max(bids.size(), asks.size()));
    }

    private static List<PriceLevel> levels(JsonNode side) {
        List<PriceLevel> out = new ArrayList<>();
        for (JsonNode level : side) {
            BigDecimal size = new BigDecimal(level.path(1).asText());
            // A zero size is a level being removed, never a resting order; mirrors the
            // Binance parser, which drops them too.
            if (size.signum() > 0) {
                out.add(new PriceLevel(new BigDecimal(level.path(0).asText()), size));
            }
        }
        return out;
    }

    /**
     * {@code BTC-USDT} to an instrument. Spot ids have exactly two parts; derivatives
     * such as {@code BTC-USDT-SWAP} are refused here rather than being mislabelled as
     * spot, since the canonical instrument has no market-type field to tell them apart.
     */
    static Instrument instrument(String instId) {
        String[] parts = instId.split("-");
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("not a spot instrument id: " + instId);
        }
        return new Instrument(instId, parts[0], parts[1], OKX);
    }

    private static BigDecimal decimal(JsonNode node, String field) {
        JsonNode v = node.path(field);
        if (!v.isTextual() || v.asText().isBlank()) {
            throw new IllegalArgumentException("missing " + field);
        }
        return new BigDecimal(v.asText());
    }
}
