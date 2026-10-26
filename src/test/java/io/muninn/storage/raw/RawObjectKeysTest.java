package io.muninn.storage.raw;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class RawObjectKeysTest {

    private static final long T = Instant.parse("2026-10-01T14:37:12.345Z").toEpochMilli();

    @Test
    void followsTheHiveLayoutFromTheStorageStrategy() {
        String key = RawObjectKeys.key("events.trade", "binance.spot.v1", "BTC-USDT", 0,
                RawObjectKeys.hourStart(T), 42, 97);
        assertThat(key).isEqualTo("events.trade/source=binance.spot.v1/instrument=BTC-USDT/"
                + "year=2026/month=10/day=01/hour=14/"
                + "part-p0-00000000000000000042-00000000000000000097.jsonl.gz");
    }

    @Test
    void sameRecordsAlwaysProduceTheSameKey() {
        // Determinism is what makes a redelivered batch overwrite instead of duplicate.
        assertThat(RawObjectKeys.key("t", "s", "i", 3, RawObjectKeys.hourStart(T), 5, 9))
                .isEqualTo(RawObjectKeys.key("t", "s", "i", 3, RawObjectKeys.hourStart(T), 5, 9));
    }

    @Test
    void hourStartTruncatesToTheUtcHour() {
        assertThat(Instant.ofEpochMilli(RawObjectKeys.hourStart(T))).isEqualTo(Instant.parse("2026-10-01T14:00:00Z"));
        long boundary = Instant.parse("2026-10-01T15:00:00Z").toEpochMilli();
        assertThat(RawObjectKeys.hourStart(boundary)).isEqualTo(boundary);
        assertThat(RawObjectKeys.hourStart(boundary - 1)).isEqualTo(RawObjectKeys.hourStart(T));
    }

    @Test
    void offsetsAreZeroPaddedSoListingOrderIsOffsetOrder() {
        String low = RawObjectKeys.key("t", "s", "i", 0, 0, 9, 9);
        String high = RawObjectKeys.key("t", "s", "i", 0, 0, 10, 10);
        assertThat(low).isLessThan(high);
    }

    @Test
    void slashesAndEqualsCannotForgeAPartition() {
        // Event data controls these strings; "/" or "=" would inject a Hive segment.
        assertThat(RawObjectKeys.segment("evil/year=1970")).isEqualTo("evil_year_1970");
        assertThat(RawObjectKeys.key("t", "a/b", "c=d", 0, 0, 1, 1)).contains("source=a_b/instrument=c_d/");
    }

    @Test
    void dotSegmentsCannotTraverseOutOfThePrefix() {
        assertThat(RawObjectKeys.segment("..")).isEqualTo(RawRecordEncoder.UNKNOWN);
        assertThat(RawObjectKeys.segment(".")).isEqualTo(RawRecordEncoder.UNKNOWN);
        assertThat(RawObjectKeys.segment("../../etc")).doesNotContain("/");
    }

    @Test
    void blankOrNullBecomesUnknown() {
        assertThat(RawObjectKeys.segment(null)).isEqualTo(RawRecordEncoder.UNKNOWN);
        assertThat(RawObjectKeys.segment("  ")).isEqualTo(RawRecordEncoder.UNKNOWN);
    }
}
