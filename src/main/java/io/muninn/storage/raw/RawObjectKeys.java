package io.muninn.storage.raw;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

/**
 * Object keys for recorded raw events, in the Hive layout from
 * {@code docs/steering/DATA_STORAGE_STRATEGY.md}:
 *
 * <pre>events.trade/source=binance.spot.v1/instrument=BTC-USDT/
 *     year=2026/month=10/day=01/hour=14/part-p0-00000000000000000042-00000000000000000097.jsonl.gz</pre>
 *
 * <p><b>Deterministic.</b> The key is a pure function of the records it holds: topic,
 * partition and offset range. A batch redelivered after a crash maps to the same key and
 * overwrites the same object rather than adding a duplicate.
 *
 * <p><b>Partitioned by arrival hour.</b> The hour comes from the Kafka record timestamp,
 * not the event's own {@code eventTime}. The record timestamp is always present and
 * assigned by the producer, while {@code eventTime} comes from venue clocks, and a units
 * mistake there (milliseconds read as microseconds, say) would file a day of data under
 * 1970. Organising by arrival is robust; the exact event time is still in every line.
 * A reader wanting an event-time window should widen it by the maximum ingestion lag.
 *
 * <p><b>Safe segments.</b> Source and instrument come from event data. A {@code /} or
 * {@code =} in either would forge an extra Hive partition, and {@code ..} would climb out
 * of the prefix, so anything outside {@code [A-Za-z0-9._-]} is replaced.
 */
public final class RawObjectKeys {

    private static final Pattern UNSAFE = Pattern.compile("[^A-Za-z0-9._-]");

    private RawObjectKeys() {}

    /** The start of the UTC hour containing {@code timestampMs}. Groups records by file. */
    public static long hourStart(long timestampMs) {
        return Math.floorDiv(timestampMs, 3_600_000L) * 3_600_000L;
    }

    public static String key(String topic, String source, String instrument, int partition,
                             long hourStartMs, long firstOffset, long lastOffset) {
        ZonedDateTime hour = Instant.ofEpochMilli(hourStartMs).atZone(ZoneOffset.UTC);
        return segment(topic)
                + "/source=" + segment(source)
                + "/instrument=" + segment(instrument)
                + String.format("/year=%04d/month=%02d/day=%02d/hour=%02d",
                        hour.getYear(), hour.getMonthValue(), hour.getDayOfMonth(), hour.getHour())
                // Offsets zero-padded so a lexical listing is also offset order.
                + String.format("/part-p%d-%020d-%020d.jsonl.gz", partition, firstOffset, lastOffset);
    }

    static String segment(String value) {
        if (value == null || value.isBlank()) {
            return RawRecordEncoder.UNKNOWN;
        }
        String safe = UNSAFE.matcher(value).replaceAll("_");
        // A segment of only dots is a path traversal even after the replacement above.
        if (safe.chars().allMatch(c -> c == '.')) {
            return RawRecordEncoder.UNKNOWN;
        }
        return safe;
    }
}
