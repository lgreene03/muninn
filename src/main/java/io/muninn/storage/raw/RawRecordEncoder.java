package io.muninn.storage.raw;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Encodes one {@link RawRecord} as a single JSON line, embedding the value bytes verbatim.
 *
 * <p>Each line is an envelope carrying the Kafka coordinates, so a reader can
 * de-duplicate by {@code (topic, partition, offset)}. The recorder writes
 * at-least-once; that pair is what makes it exactly-once at read time.
 *
 * <pre>{"topic":"events.trade","partition":0,"offset":42,"timestamp":1759320000123,
 *  "key":"BTC-USDT","value":{...the exact bytes the broker held...}}</pre>
 *
 * <p>The value is spliced in as raw bytes, not re-serialised. Re-serialising through
 * a parsed tree is lossy: a number such as {@code 0.00150} can come back as
 * {@code 0.0015}, discarding the precision the venue quoted.
 *
 * <p>Nothing is ever dropped. A value that is not a single valid JSON document is
 * written as {@code valueBase64} and routed to {@value #UNPARSED}, so a malformed
 * record is preserved and visible rather than lost.
 */
public final class RawRecordEncoder {

    /** Routing segment for records whose value could not be parsed as JSON. */
    public static final String UNPARSED = "_unparsed";
    /** Routing segment for a parsed value that lacks the routing field. */
    public static final String UNKNOWN = "_unknown";

    /**
     * Strict on purpose. Without {@code FAIL_ON_TRAILING_TOKENS}, {@code {"a":1} junk}
     * parses as valid, and splicing it verbatim would produce an invalid line. The
     * default also rejects unescaped control characters inside strings, which is what
     * makes newline normalisation below safe.
     */
    private static final ObjectMapper STRICT = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    private static final byte[] LINE_END = {'\n'};

    /** A ready-to-write line, plus where it should be filed. */
    public record Encoded(byte[] line, String source, String instrument, boolean parsed) {}

    public Encoded encode(RawRecord r) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(
                64 + (r.value() == null ? 0 : r.value().length));
        try {
            out.write('{');
            field(out, "topic", STRICT.writeValueAsBytes(r.topic()), false);
            field(out, "partition", Integer.toString(r.partition()).getBytes(StandardCharsets.US_ASCII), true);
            field(out, "offset", Long.toString(r.offset()).getBytes(StandardCharsets.US_ASCII), true);
            field(out, "timestamp", Long.toString(r.timestamp()).getBytes(StandardCharsets.US_ASCII), true);
            field(out, "key", r.key() == null
                    ? "null".getBytes(StandardCharsets.US_ASCII)
                    : STRICT.writeValueAsBytes(r.key()), true);

            if (r.value() == null) {
                // A tombstone. Nothing to route on, but it is still a fact worth keeping.
                field(out, "value", "null".getBytes(StandardCharsets.US_ASCII), true);
                out.write('}');
                out.write(LINE_END);
                return new Encoded(out.toByteArray(), UNKNOWN, UNKNOWN, false);
            }

            JsonNode tree = parseObject(r.value());
            if (tree == null) {
                field(out, "valueBase64",
                        STRICT.writeValueAsBytes(Base64.getEncoder().encodeToString(r.value())), true);
                out.write('}');
                out.write(LINE_END);
                return new Encoded(out.toByteArray(), UNPARSED, UNPARSED, false);
            }

            field(out, "value", withoutLineBreaks(r.value()), true);
            out.write('}');
            out.write(LINE_END);
            return new Encoded(out.toByteArray(), text(tree.path("source")), text(tree.path("instrument").path("symbol")), true);
        } catch (IOException e) {
            // Only reachable from writeValueAsBytes on a String, which cannot fail.
            throw new UncheckedIOException(e);
        }
    }

    private static void field(ByteArrayOutputStream out, String name, byte[] jsonValue, boolean comma)
            throws IOException {
        if (comma) {
            out.write(',');
        }
        out.write('"');
        out.write(name.getBytes(StandardCharsets.US_ASCII));
        out.write('"');
        out.write(':');
        out.write(jsonValue);
    }

    /** The value as an object node, or null if it is not exactly one JSON object. */
    private static JsonNode parseObject(byte[] value) {
        try {
            JsonNode node = STRICT.readTree(value);
            return node != null && node.isObject() ? node : null;
        } catch (JsonProcessingException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Replace raw CR and LF bytes with spaces, so a pretty-printed value cannot break
     * the one-record-per-line framing.
     *
     * <p>Safe only because {@link #parseObject} already accepted the document under a
     * parser that rejects unescaped control characters inside strings. In such a
     * document every raw CR or LF is whitespace between tokens, so swapping it for a
     * space changes no string, no number and no structure. Compact JSON, which is what
     * the producer writes, contains none and is copied untouched.
     */
    static byte[] withoutLineBreaks(byte[] value) {
        boolean found = false;
        for (byte b : value) {
            if (b == '\n' || b == '\r') {
                found = true;
                break;
            }
        }
        if (!found) {
            return value;
        }
        byte[] copy = value.clone();
        for (int i = 0; i < copy.length; i++) {
            if (copy[i] == '\n' || copy[i] == '\r') {
                copy[i] = ' ';
            }
        }
        return copy;
    }

    private static String text(JsonNode node) {
        return node.isTextual() && !node.asText().isBlank() ? node.asText() : UNKNOWN;
    }
}
