package io.muninn.ingestion.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.muninn.shared.event.MarketEvent;
import io.muninn.shared.instrument.Exchange;
import io.muninn.shared.instrument.Instrument;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * One Binance combined stream carrying every configured instrument: the path that
 * subscribes to it, and the attribution of each message to the instrument its
 * {@code stream} field names. Pure apart from its sequence counters: no I/O.
 *
 * <p>Until G0.18 the adapter subscribed to the first configured instrument only and
 * labelled every message with that instrument, whatever the message said. Configuring
 * a second instrument recorded nothing for it, and anything else arriving on the
 * socket would have been filed under the first. Now:
 *
 * <ul>
 *   <li>every configured instrument is subscribed to every configured stream, on one
 *       connection: {@code /stream?streams=btcusdt@trade/btcusdt@depth20@100ms/ethusdt@trade/...};</li>
 *   <li>a message is attributed only through its {@code stream} field, looked up exactly
 *       among the streams subscribed. A message whose stream is missing or was never
 *       subscribed is reported and dropped, never given a default instrument;</li>
 *   <li>a trade whose payload symbol ({@code s}) disagrees with its stream is refused.
 *       Partial book depth payloads carry no symbol, so for books the stream name is the
 *       only evidence;</li>
 *   <li>sequence numbers count per instrument and per event type, so each instrument's
 *       trades, and separately its books, number 1, 2, 3 without gaps. That is the
 *       {@code (source, instrument)} series the event contract describes, and it keeps
 *       the existing split between trades and books. A shared counter would still be
 *       monotonic per instrument, but its gaps would look like lost messages. A
 *       message that is routed but fails to parse still uses its number, so the gap
 *       it leaves marks a real loss.</li>
 * </ul>
 *
 * <p>Limits, from Binance's spot WebSocket stream documentation (checked 2026-11-10):
 * symbols in stream names are lowercase; one connection carries at most 1024 streams,
 * which the constructor enforces rather than letting Binance refuse the connection at
 * run time; and a connection lasts at most 24 hours, after which the adapter's
 * reconnect takes over. Binance documents no limit on URL length. Each stream adds about
 * 20 characters to the request line, and many servers refuse request lines beyond 8 KB,
 * roughly 350 streams, so a configuration that large should subscribe with the
 * {@code SUBSCRIBE} method or over several connections instead. The default, five
 * instruments with two streams each, is ten streams and under 250 characters.
 */
final class BinanceCombinedStream {

    static final String SOURCE = "binance.spot.v1";
    static final int MAX_STREAMS_PER_CONNECTION = 1024;

    private static final Exchange BINANCE = new Exchange("binance", "Binance Spot", ZoneId.of("UTC"));
    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Partial book depth, the only depth stream that carries a full snapshot. */
    private static final Pattern PARTIAL_DEPTH = Pattern.compile("depth(5|10|20)(@100ms)?");

    /** An event, or the reason the message produced none. Exactly one is non-null. */
    record Result(MarketEvent event, String problem) {
        static Result of(MarketEvent event) {
            return new Result(event, null);
        }

        static Result problem(String why) {
            return new Result(null, why);
        }
    }

    private enum Kind { TRADE, BOOK }

    private record Route(Instrument instrument, String symbolUpper, Kind kind, AtomicLong sequence) {}

    private final Map<String, Route> routes;
    private final String path;

    /**
     * @param instruments Binance symbols, such as {@code btcusdt}; case does not matter
     * @param streams     stream types, each {@code trade} or a partial book depth such as
     *                    {@code depth20@100ms}
     * @throws IllegalArgumentException if there are no instruments or streams, a stream
     *                                  type cannot be parsed, or the total exceeds what one
     *                                  connection may carry
     */
    BinanceCombinedStream(List<String> instruments, List<String> streams) {
        Set<String> symbols = new LinkedHashSet<>();
        for (String instrument : instruments) {
            symbols.add(instrument.trim().toLowerCase(Locale.ROOT));
        }
        Set<String> types = new LinkedHashSet<>(streams);
        if (symbols.isEmpty() || types.isEmpty()) {
            throw new IllegalArgumentException("Binance needs at least one instrument and one stream; got instruments "
                    + instruments + " and streams " + streams);
        }
        int total = symbols.size() * types.size();
        if (total > MAX_STREAMS_PER_CONNECTION) {
            throw new IllegalArgumentException(symbols.size() + " instruments x " + types.size() + " streams = "
                    + total + " streams, but one Binance connection carries at most "
                    + MAX_STREAMS_PER_CONNECTION);
        }

        Map<String, Route> byStream = new LinkedHashMap<>();
        Map<String, AtomicLong> sequences = new HashMap<>();
        for (String symbol : symbols) {
            Instrument instrument = instrument(symbol);
            String symbolUpper = symbol.toUpperCase(Locale.ROOT);
            for (String type : types) {
                Kind kind = kind(type);
                AtomicLong sequence = sequences.computeIfAbsent(symbol + "/" + kind, k -> new AtomicLong());
                byStream.put(symbol + "@" + type, new Route(instrument, symbolUpper, kind, sequence));
            }
        }
        this.routes = Map.copyOf(byStream);
        this.path = "/stream?streams=" + String.join("/", byStream.keySet());
    }

    /** The combined stream path to append to the base URL. */
    String path() {
        return path;
    }

    /** Attributes one raw message to its instrument and parses it. Never throws. */
    Result route(String rawMessage, Instant ingestTime) {
        JsonNode root;
        try {
            root = MAPPER.readTree(rawMessage);
        } catch (Exception e) {
            return Result.problem("not JSON: " + e.getMessage());
        }
        if (root == null || !root.path("stream").isTextual() || !root.path("data").isObject()) {
            return Result.problem("not a combined stream message, so it names no instrument");
        }
        String stream = root.path("stream").asText();
        Route route = routes.get(stream);
        if (route == null) {
            return Result.problem("stream " + stream + " was not subscribed");
        }
        JsonNode data = root.path("data");
        try {
            String payload = MAPPER.writeValueAsString(data);
            return switch (route.kind()) {
                case TRADE -> {
                    String symbol = data.path("s").asText("");
                    if (!symbol.isEmpty() && !symbol.equals(route.symbolUpper())) {
                        yield Result.problem("trade for " + symbol + " arrived on stream " + stream);
                    }
                    yield Result.of(BinanceTradeParser.parse(payload, route.instrument(), SOURCE, ingestTime,
                            route.sequence().incrementAndGet()));
                }
                case BOOK -> Result.of(BinanceBookParser.parse(payload, route.instrument(), SOURCE, ingestTime,
                        route.sequence().incrementAndGet()));
            };
        } catch (Exception e) {
            return Result.problem("stream " + stream + ": " + e.getMessage());
        }
    }

    private static Kind kind(String type) {
        if ("trade".equals(type)) {
            return Kind.TRADE;
        }
        if (PARTIAL_DEPTH.matcher(type).matches()) {
            return Kind.BOOK;
        }
        throw new IllegalArgumentException("Binance stream type '" + type + "' is not supported: use trade or a"
                + " partial book depth (depth5, depth10 or depth20, optionally @100ms)");
    }

    /** {@code btcusdt} to {@code BTC-USDT} on Binance; unchanged from the adapter's earlier mapping. */
    private static Instrument instrument(String symbol) {
        String upper = symbol.toUpperCase(Locale.ROOT);
        String base;
        String quote;
        if (upper.endsWith("USDT")) {
            base = upper.substring(0, upper.length() - 4);
            quote = "USDT";
        } else if (upper.endsWith("USD")) {
            base = upper.substring(0, upper.length() - 3);
            quote = "USD";
        } else {
            base = upper;
            quote = "UNKNOWN";
        }
        return new Instrument(base + "-" + quote, base, quote, BINANCE);
    }
}
