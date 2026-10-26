package io.muninn.storage.raw;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The recorder's guarantees, driven deterministically: a real Kafka MockConsumer, a putter
 * that can be broken and healed, and a clock the test moves by hand.
 */
class RawEventRecorderTest {

    private static final TopicPartition TP0 = new TopicPartition("events.trade", 0);
    private static final TopicPartition TP1 = new TopicPartition("events.trade", 1);
    private static final long TS = Instant.parse("2026-10-01T14:10:00Z").toEpochMilli();
    private static final Duration AGE = Duration.ofSeconds(60);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

    private MockConsumer<String, byte[]> consumer;
    private RawBatchWriterTest.FakePutter putter;
    private BreakablePutter storage;
    private AtomicLong now;
    private RawEventRecorder recorder;

    /** Healthy unless told otherwise; counts every attempt. */
    static final class BreakablePutter implements ObjectPutter {
        final RawBatchWriterTest.FakePutter delegate = new RawBatchWriterTest.FakePutter();
        boolean down;
        int attempts;

        @Override
        public void put(String key, byte[] body) {
            attempts++;
            if (down) {
                throw new IllegalStateException("object storage down");
            }
            delegate.put(key, body);
        }
    }

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>("earliest");
        storage = new BreakablePutter();
        putter = storage.delegate;
        now = new AtomicLong(1_000_000_000L);
        recorder = newRecorder(Long.MAX_VALUE);
        recorder.subscribe();
        consumer.rebalance(List.of(TP0, TP1));
        consumer.updateBeginningOffsets(Map.of(TP0, 0L, TP1, 0L));
    }

    private RawEventRecorder newRecorder(long maxBufferBytes) {
        RawRecorderProperties props = new RawRecorderProperties(true, List.of("events.trade"),
                "g", "muninn-raw", maxBufferBytes, AGE, MAX_BACKOFF);
        return new RawEventRecorder(consumer, new RawBatchWriter(storage), props,
                new SimpleMeterRegistry(), now::get);
    }

    private void add(TopicPartition tp, long offset) {
        byte[] value = ("{\"source\":\"binance.spot.v1\",\"instrument\":{\"symbol\":\"BTC-USDT\"},\"o\":" + offset + "}")
                .getBytes(StandardCharsets.UTF_8);
        consumer.addRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, TS,
                TimestampType.CREATE_TIME, 0, value.length, "BTC-USDT", value, new RecordHeaders(), Optional.empty()));
    }

    private void advance(Duration d) {
        now.addAndGet(d.toNanos());
    }

    private Long committed(TopicPartition tp) {
        OffsetAndMetadata o = consumer.committed(Set.of(tp)).get(tp);
        return o == null ? null : o.offset();
    }

    @Test
    void doesNotFlushOrCommitBeforeTheBufferIsDue() {
        add(TP0, 0);
        recorder.pollOnce();
        assertThat(putter.objects).isEmpty();
        assertThat(committed(TP0)).isNull();
        assertThat(recorder.bufferedRecords()).isEqualTo(1);
    }

    @Test
    void commitsTheNextOffsetOnlyAfterStoring() {
        add(TP0, 0);
        add(TP0, 1);
        add(TP0, 2);
        recorder.pollOnce();
        advance(AGE);
        recorder.pollOnce();

        assertThat(putter.objects).hasSize(1);
        assertThat(committed(TP0)).as("next offset to read, one past the last stored").isEqualTo(3L);
        assertThat(recorder.bufferedRecords()).isZero();
    }

    @Test
    void storageFailureCommitsNothingAndKeepsEveryRecord() {
        add(TP0, 0);
        add(TP0, 1);
        recorder.pollOnce();
        storage.down = true;
        advance(AGE);
        recorder.pollOnce();

        assertThat(recorder.isFailing()).isTrue();
        assertThat(committed(TP0)).as("nothing may be committed for unstored data").isNull();
        assertThat(recorder.bufferedRecords()).as("held, never dropped").isEqualTo(2);
    }

    @Test
    void failurePausesConsumptionSoTheBufferCannotGrow() {
        add(TP0, 0);
        recorder.pollOnce();
        storage.down = true;
        advance(AGE);
        recorder.pollOnce();
        assertThat(consumer.paused()).contains(TP0, TP1);

        // New data arrives during the outage; a paused consumer must not take it.
        add(TP0, 1);
        add(TP0, 2);
        recorder.pollOnce();
        assertThat(recorder.bufferedRecords()).as("backpressure, not unbounded growth").isEqualTo(1);
    }

    @Test
    void doesNotRetryBeforeTheBackoffElapses() {
        add(TP0, 0);
        recorder.pollOnce();
        storage.down = true;
        advance(AGE);
        recorder.pollOnce();
        int afterFirstFailure = storage.attempts;

        recorder.pollOnce();
        recorder.pollOnce();
        assertThat(storage.attempts).as("no hammering a failing store").isEqualTo(afterFirstFailure);
    }

    @Test
    void recoversWithNoLossAndResumes() {
        add(TP0, 0);
        add(TP0, 1);
        recorder.pollOnce();
        storage.down = true;
        advance(AGE);
        recorder.pollOnce();
        add(TP0, 2); // arrives while paused

        storage.down = false;
        advance(MAX_BACKOFF);
        recorder.pollOnce();

        assertThat(recorder.isFailing()).isFalse();
        assertThat(consumer.paused()).isEmpty();
        assertThat(committed(TP0)).isEqualTo(2L);

        // The record that arrived during the outage is picked up after resuming.
        recorder.pollOnce();
        advance(AGE);
        recorder.pollOnce();
        assertThat(committed(TP0)).isEqualTo(3L);
        long stored = putter.objects.values().stream()
                .mapToLong(o -> RawBatchWriterTest.gunzipLines(o).size()).sum();
        assertThat(stored).as("every record stored exactly once overall").isEqualTo(3);
    }

    @Test
    void backoffGrowsButIsCapped() {
        add(TP0, 0);
        recorder.pollOnce();
        storage.down = true;
        advance(AGE);
        recorder.pollOnce();

        // Ten more failures; each retry must wait no longer than the cap.
        for (int i = 0; i < 10; i++) {
            int before = storage.attempts;
            advance(MAX_BACKOFF);
            recorder.pollOnce();
            assertThat(storage.attempts).as("retried within maxBackoff on round " + i).isGreaterThan(before);
        }
    }

    @Test
    void flushesBySizeWithoutWaitingForAge() {
        // Re-subscribing replaces setUp's recorder as the consumer's rebalance listener.
        recorder = newRecorder(1); // any record exceeds one byte
        recorder.subscribe();
        consumer.rebalance(List.of(TP0, TP1));
        add(TP0, 0);
        recorder.pollOnce();
        assertThat(committed(TP0)).isEqualTo(1L);
    }

    @Test
    void drainStoresWhatIsBufferedAtShutdown() {
        add(TP0, 0);
        add(TP1, 0);
        recorder.pollOnce();
        recorder.drain(3);
        assertThat(committed(TP0)).isEqualTo(1L);
        assertThat(committed(TP1)).isEqualTo(1L);
        assertThat(recorder.bufferedRecords()).isZero();
    }

    @Test
    void drainDuringAnOutageLosesNothingBecauseNothingIsCommitted() {
        add(TP0, 0);
        recorder.pollOnce();
        storage.down = true;
        recorder.drain(3);
        assertThat(committed(TP0)).as("uncommitted, so redelivered on restart").isNull();
    }

    @Test
    void revokingAPartitionDuringAnOutageReleasesOnlyThatPartitionsRecords() {
        add(TP0, 0);
        add(TP1, 0);
        recorder.pollOnce();
        storage.down = true;
        advance(AGE);
        recorder.pollOnce();
        assertThat(recorder.bufferedRecords()).isEqualTo(2);

        consumer.rebalance(List.of(TP1)); // TP0 moves to another consumer

        assertThat(recorder.bufferedRecords()).as("TP0's record is the new owner's to read").isEqualTo(1);
        assertThat(committed(TP0)).as("and it was never committed, so it will be read").isNull();
    }
}
