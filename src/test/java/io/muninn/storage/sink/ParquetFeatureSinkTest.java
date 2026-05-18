package io.muninn.storage.sink;

import io.muninn.shared.event.FeatureComputedEvent;
import io.muninn.shared.time.UUIDv7;
import io.muninn.storage.FeatureParquetWriter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ParquetFeatureSinkTest {

    @Test
    void sinkId_isParquet() {
        ParquetFeatureSink sink = new ParquetFeatureSink(new RecordingWriter("dummy-key"));
        assertThat(sink.sinkId()).isEqualTo("parquet");
    }

    @Test
    void write_delegatesToFeatureParquetWriterAndReturnsResult() {
        String returnedKey = "features.vwap.1m.v1/instrument=BTC-USDT/.../part-00000.parquet";
        RecordingWriter writer = new RecordingWriter(returnedKey);
        ParquetFeatureSink sink = new ParquetFeatureSink(writer);

        FeatureSink.SinkWriteResult result = sink.write("BTC-USDT", List.of(sampleEvent()));

        assertThat(writer.lastInstrument.get()).isEqualTo("BTC-USDT");
        assertThat(writer.lastBatchSize.get()).isEqualTo(1);
        assertThat(result.sinkId()).isEqualTo("parquet");
        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.location()).isEqualTo(returnedKey);
    }

    @Test
    void write_rejectsEmptyBatch() {
        ParquetFeatureSink sink = new ParquetFeatureSink(new RecordingWriter("unused"));

        assertThatThrownBy(() -> sink.write("BTC-USDT", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty");
    }

    private FeatureComputedEvent sampleEvent() {
        Instant t = Instant.parse("2026-05-10T14:00:00Z");
        return new FeatureComputedEvent(
                UUIDv7.generate(),
                t,
                "vwap.1m",
                "v1",
                new BigDecimal("60000.00"),
                null,
                t,
                t.plusSeconds(60),
                List.<UUID>of(),
                "dev"
        );
    }

    /**
     * Test double — captures the call args so the sink's contract can be
     * asserted without mocking. Avoids Mockito's byte-buddy self-attach
     * which is restricted on newer JDKs.
     */
    private static final class RecordingWriter extends FeatureParquetWriter {
        final AtomicReference<String> lastInstrument = new AtomicReference<>();
        final AtomicReference<Integer> lastBatchSize = new AtomicReference<>();
        private final String returnedKey;

        RecordingWriter(String returnedKey) {
            super(null, "test-bucket");
            this.returnedKey = returnedKey;
        }

        @Override
        public String write(String instrument, List<FeatureComputedEvent> events) {
            lastInstrument.set(instrument);
            lastBatchSize.set(events.size());
            return returnedKey;
        }
    }
}
