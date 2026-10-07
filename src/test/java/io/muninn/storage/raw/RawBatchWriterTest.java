package io.muninn.storage.raw;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class RawBatchWriterTest {

    private static final long HOUR_14 = Instant.parse("2026-10-01T14:10:00Z").toEpochMilli();
    private static final long HOUR_15 = Instant.parse("2026-10-01T15:05:00Z").toEpochMilli();

    /** Captures objects; can be told to fail on the Nth put. */
    static final class FakePutter implements ObjectPutter {
        final Map<String, byte[]> objects = new LinkedHashMap<>();
        int failOnCall = -1;
        int calls;

        @Override
        public void put(String key, byte[] body) {
            calls++;
            if (calls == failOnCall) {
                throw new IllegalStateException("storage unavailable");
            }
            objects.put(key, body);
        }
    }

    private static RawRecord trade(String topic, int partition, long offset, long ts, String source, String symbol) {
        String json = "{\"source\":\"" + source + "\",\"instrument\":{\"symbol\":\"" + symbol + "\"},\"n\":" + offset + "}";
        return new RawRecord(topic, partition, offset, ts, symbol, json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void oneObjectPerTopicPartitionVenueInstrumentAndHour() {
        FakePutter putter = new FakePutter();
        new RawBatchWriter(putter).write(List.of(
                trade("events.trade", 0, 1, HOUR_14, "binance.spot.v1", "BTC-USDT"),
                trade("events.trade", 0, 2, HOUR_14, "binance.spot.v1", "BTC-USDT"),
                trade("events.trade", 0, 3, HOUR_14, "coinbase.spot.v1", "BTC-USD"),
                trade("events.trade", 1, 1, HOUR_14, "binance.spot.v1", "BTC-USDT"),
                trade("events.trade", 0, 4, HOUR_15, "binance.spot.v1", "BTC-USDT")));

        assertThat(putter.objects.keySet()).containsExactlyInAnyOrder(
                "events.trade/source=binance.spot.v1/instrument=BTC-USDT/year=2026/month=10/day=01/hour=14/part-p0-00000000000000000001-00000000000000000002.jsonl.gz",
                "events.trade/source=coinbase.spot.v1/instrument=BTC-USD/year=2026/month=10/day=01/hour=14/part-p0-00000000000000000003-00000000000000000003.jsonl.gz",
                "events.trade/source=binance.spot.v1/instrument=BTC-USDT/year=2026/month=10/day=01/hour=14/part-p1-00000000000000000001-00000000000000000001.jsonl.gz",
                "events.trade/source=binance.spot.v1/instrument=BTC-USDT/year=2026/month=10/day=01/hour=15/part-p0-00000000000000000004-00000000000000000004.jsonl.gz");
    }

    @Test
    void objectDecompressesToEveryLineInOffsetOrder() throws IOException {
        FakePutter putter = new FakePutter();
        // Deliberately out of order: file content must not depend on batch assembly.
        new RawBatchWriter(putter).write(List.of(
                trade("events.trade", 0, 3, HOUR_14, "s", "i"),
                trade("events.trade", 0, 1, HOUR_14, "s", "i"),
                trade("events.trade", 0, 2, HOUR_14, "s", "i")));

        List<String> lines = gunzipLines(putter.objects.values().iterator().next());
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).contains("\"offset\":1,");
        assertThat(lines.get(1)).contains("\"offset\":2,");
        assertThat(lines.get(2)).contains("\"offset\":3,");
    }

    @Test
    void reportsTheHighestOffsetWrittenPerPartition() {
        RawBatchWriter.Result result = new RawBatchWriter(new FakePutter()).write(List.of(
                trade("events.trade", 0, 5, HOUR_14, "s", "i"),
                trade("events.trade", 0, 9, HOUR_15, "s", "i"),
                trade("events.trade", 1, 2, HOUR_14, "s", "i"),
                trade("events.book.snapshot", 0, 7, HOUR_14, "s", "i")));

        assertThat(result.lastOffsetWritten()).containsExactlyInAnyOrderEntriesOf(Map.of(
                new TopicPartition("events.trade", 0), 9L,
                new TopicPartition("events.trade", 1), 2L,
                new TopicPartition("events.book.snapshot", 0), 7L));
        assertThat(result.records()).isEqualTo(4);
    }

    @Test
    void rewritingTheSameBatchProducesIdenticalObjects() {
        // Redelivery after a crash must overwrite, not add a near-duplicate.
        List<RawRecord> batch = List.of(
                trade("events.trade", 0, 1, HOUR_14, "s", "i"),
                trade("events.trade", 0, 2, HOUR_14, "s", "i"));
        FakePutter first = new FakePutter();
        FakePutter second = new FakePutter();
        new RawBatchWriter(first).write(batch);
        new RawBatchWriter(second).write(batch);

        assertThat(second.objects.keySet()).isEqualTo(first.objects.keySet());
        String key = first.objects.keySet().iterator().next();
        assertThat(gunzipLines(second.objects.get(key))).isEqualTo(gunzipLines(first.objects.get(key)));
    }

    @Test
    void aFailedPutFailsTheWholeWriteSoNothingIsCommitted() {
        FakePutter putter = new FakePutter();
        putter.failOnCall = 2;
        RawBatchWriter writer = new RawBatchWriter(putter);
        assertThatThrownBy(() -> writer.write(List.of(
                trade("events.trade", 0, 1, HOUR_14, "a", "i"),
                trade("events.trade", 0, 2, HOUR_14, "b", "i"))))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unparseableValuesAreStoredAndCounted() throws IOException {
        FakePutter putter = new FakePutter();
        RawBatchWriter.Result result = new RawBatchWriter(putter).write(List.of(
                new RawRecord("events.trade", 0, 1, HOUR_14, null, new byte[] {(byte) 0xff, 0x01})));
        assertThat(result.unparsed()).isEqualTo(1);
        assertThat(putter.objects.keySet().iterator().next()).contains("source=_unparsed/instrument=_unparsed/");
        assertThat(gunzipLines(putter.objects.values().iterator().next()).getFirst()).contains("valueBase64");
    }

    static List<String> gunzipLines(byte[] gz) {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(gz))) {
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            List<String> lines = new ArrayList<>(List.of(text.split("\n")));
            lines.removeIf(String::isEmpty);
            return lines;
        } catch (IOException e) {
            throw new AssertionError("object is not valid gzip", e);
        }
    }
}
