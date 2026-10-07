package io.muninn.storage.raw;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class RawRecordEncoderTest {

    /**
     * Reads numbers exactly, so a test can see precision.
     *
     * <p>{@code USE_BIG_DECIMAL_FOR_FLOATS} alone is NOT enough: Jackson's tree model
     * strips trailing BigDecimal zeros by default, so {@code 0.00150} reads back as
     * {@code 0.0015} even as a BigDecimal. Found when these tests first failed. It is
     * the same hazard the encoder exists to avoid, sitting in default library behaviour.
     */
    private static final ObjectMapper EXACT = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
            .build();

    private final RawRecordEncoder encoder = new RawRecordEncoder();

    private static RawRecord record(String value) {
        return new RawRecord("events.trade", 2, 42L, 1_759_320_000_123L, "BTC-USDT",
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    private static final String TRADE = "{\"source\":\"binance.spot.v1\","
            + "\"instrument\":{\"symbol\":\"BTC-USDT\",\"exchange\":{\"id\":\"binance\"}},"
            + "\"price\":67523.45,\"size\":0.00150,\"side\":\"BUY\"}";

    @Test
    void valueBytesAreEmbeddedVerbatim() {
        // The whole point: the stored value is the broker's bytes, not a re-rendering.
        String line = new String(encoder.encode(record(TRADE)).line(), StandardCharsets.UTF_8);
        assertThat(line).contains("\"value\":" + TRADE + "}");
    }

    @Test
    void trailingZeroPrecisionSurvives() {
        // 0.00150 re-serialised through a double becomes 0.0015, losing the venue's quoted
        // scale. Read back exactly, the scale must still be 5.
        JsonNode node = parse(encoder.encode(record(TRADE)).line());
        assertThat(node.path("value").path("size").decimalValue()).isEqualByComparingTo("0.00150");
        assertThat(node.path("value").path("size").decimalValue().scale()).isEqualTo(5);
    }

    @Test
    void reserialisingThroughADefaultTreeWouldHaveLostThePrecision() throws Exception {
        // Pins WHY the encoder splices bytes instead of parsing and re-writing. If this
        // ever stops failing the round trip, the reason for splicing has gone; until
        // then, "simplifying" the encoder to re-serialise would corrupt history.
        ObjectMapper ordinary = JsonMapper.builder()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .build();
        String roundTripped = ordinary.writeValueAsString(ordinary.readTree(TRADE));
        assertThat(roundTripped).doesNotContain("0.00150").contains("0.0015");
        // Whereas the encoder keeps the broker's bytes.
        assertThat(new String(encoder.encode(record(TRADE)).line(), StandardCharsets.UTF_8)).contains("0.00150");
    }

    @Test
    void envelopeCarriesKafkaCoordinatesForExactDeduplication() {
        JsonNode node = parse(encoder.encode(record(TRADE)).line());
        assertThat(node.path("topic").asText()).isEqualTo("events.trade");
        assertThat(node.path("partition").asInt()).isEqualTo(2);
        assertThat(node.path("offset").asLong()).isEqualTo(42L);
        assertThat(node.path("timestamp").asLong()).isEqualTo(1_759_320_000_123L);
        assertThat(node.path("key").asText()).isEqualTo("BTC-USDT");
    }

    @Test
    void routesOnSourceAndInstrument() {
        RawRecordEncoder.Encoded e = encoder.encode(record(TRADE));
        assertThat(e.parsed()).isTrue();
        assertThat(e.source()).isEqualTo("binance.spot.v1");
        assertThat(e.instrument()).isEqualTo("BTC-USDT");
    }

    @Test
    void everyLineIsExactlyOneLineEvenForPrettyPrintedValues() {
        String pretty = "{\n  \"source\" : \"binance.spot.v1\",\r\n  \"size\" : 0.00150\n}";
        byte[] line = encoder.encode(record(pretty)).line();
        String text = new String(line, StandardCharsets.UTF_8);
        assertThat(text.chars().filter(c -> c == '\n').count()).as("only the terminator").isEqualTo(1);
        assertThat(text).doesNotContain("\r").endsWith("\n");
        // Normalising inter-token whitespace must not change any value.
        JsonNode node = parse(line);
        assertThat(node.path("value").path("size").decimalValue().scale()).isEqualTo(5);
        assertThat(node.path("value").path("source").asText()).isEqualTo("binance.spot.v1");
    }

    @Test
    void escapedNewlinesInsideStringsAreLeftAlone() {
        // An escaped \n inside a string is two characters, backslash and n, not a raw LF.
        String withEscape = "{\"note\":\"line one\\nline two\",\"source\":\"x\"}";
        JsonNode node = parse(encoder.encode(record(withEscape)).line());
        assertThat(node.path("value").path("note").asText()).isEqualTo("line one\nline two");
    }

    @Test
    void unicodeSurvivesByteForByte() {
        String unicode = "{\"source\":\"vénue.spot.v1\",\"instrument\":{\"symbol\":\"币-USDT\"}}";
        String line = new String(encoder.encode(record(unicode)).line(), StandardCharsets.UTF_8);
        assertThat(line).contains(unicode);
    }

    @Test
    void invalidJsonIsPreservedAsBase64AndRoutedSeparately() {
        byte[] garbage = {(byte) 0xff, 0x00, 'n', 'o', 't', ' ', 'j', 's', 'o', 'n'};
        RawRecord r = new RawRecord("events.trade", 0, 7L, 1L, null, garbage);
        RawRecordEncoder.Encoded e = encoder.encode(r);
        assertThat(e.parsed()).isFalse();
        assertThat(e.source()).isEqualTo(RawRecordEncoder.UNPARSED);
        JsonNode node = parse(e.line());
        assertThat(node.has("value")).isFalse();
        assertThat(Base64.getDecoder().decode(node.path("valueBase64").asText())).isEqualTo(garbage);
    }

    @Test
    void trailingTokensAfterValidJsonAreNotSplicedIn() {
        // A lenient parser accepts this; splicing it verbatim would corrupt the line.
        RawRecordEncoder.Encoded e = encoder.encode(record("{\"source\":\"x\"} trailing junk"));
        assertThat(e.parsed()).isFalse();
        assertThat(parse(e.line()).has("valueBase64")).isTrue();
    }

    @Test
    void aJsonValueThatIsNotAnObjectIsNotRoutedAsAnEvent() {
        assertThat(encoder.encode(record("[1,2,3]")).parsed()).isFalse();
        assertThat(encoder.encode(record("42")).parsed()).isFalse();
    }

    @Test
    void tombstoneIsRecordedNotDropped() {
        RawRecordEncoder.Encoded e = encoder.encode(record(null));
        assertThat(parse(e.line()).path("value").isNull()).isTrue();
        assertThat(e.source()).isEqualTo(RawRecordEncoder.UNKNOWN);
    }

    @Test
    void missingRoutingFieldsFallBackToUnknownRatherThanFailing() {
        RawRecordEncoder.Encoded e = encoder.encode(record("{\"price\":1}"));
        assertThat(e.parsed()).isTrue();
        assertThat(e.source()).isEqualTo(RawRecordEncoder.UNKNOWN);
        assertThat(e.instrument()).isEqualTo(RawRecordEncoder.UNKNOWN);
    }

    @Test
    void nullKeyIsEncodedAsJsonNull() {
        RawRecord r = new RawRecord("events.trade", 0, 1L, 1L, null, TRADE.getBytes(StandardCharsets.UTF_8));
        assertThat(parse(encoder.encode(r).line()).path("key").isNull()).isTrue();
    }

    @Test
    void keyWithQuotesIsEscapedNotSpliced() {
        RawRecord r = new RawRecord("events.trade", 0, 1L, 1L, "a\"b",
                TRADE.getBytes(StandardCharsets.UTF_8));
        assertThat(parse(encoder.encode(r).line()).path("key").asText()).isEqualTo("a\"b");
    }

    private static JsonNode parse(byte[] line) {
        try {
            return EXACT.readTree(line);
        } catch (Exception e) {
            throw new AssertionError("line is not valid JSON: " + new String(line, StandardCharsets.UTF_8), e);
        }
    }
}
