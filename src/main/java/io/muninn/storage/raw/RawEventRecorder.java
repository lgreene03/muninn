package io.muninn.storage.raw;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Records every raw market event to object storage, so history outlives Kafka retention.
 *
 * <p>Before this existed nothing recorded raw events at all: the only history was Kafka,
 * at 24 hours for trades. Every day without a recorder is history that cannot be
 * recovered, which is why this was built ahead of adding venues.
 *
 * <h2>Guarantees</h2>
 * <ul>
 *   <li><b>No loss.</b> Offsets are committed only after every object in a flush has been
 *       stored. A crash at any point leaves uncommitted records that Kafka redelivers.</li>
 *   <li><b>No silent drop.</b> When storage fails the buffer is kept, the assigned
 *       partitions are paused, and the flush is retried with capped exponential backoff,
 *       indefinitely. Nothing is discarded to make progress. The existing feature
 *       archival consumer clears its buffer on failure, which is exactly what this must
 *       not do.</li>
 *   <li><b>Bounded memory.</b> While paused no new records arrive, so an outage applies
 *       backpressure instead of growing the buffer.</li>
 *   <li><b>Exactly once at read time.</b> Writes are at-least-once; every line carries
 *       {@code (topic, partition, offset)}, so a reader de-duplicates exactly.</li>
 * </ul>
 *
 * <h2>Shape</h2>
 * <p>A raw consumer on a dedicated thread, the same pattern as {@code LiveEventSource},
 * rather than a Spring batch listener. Spring's default batch error handling gives up
 * after its retries and commits past the failed batch, which for a recorder means
 * silently skipping data. A raw loop keeps every edge explicit.
 *
 * <p>The retry is a non-blocking state machine, not a sleep. While failing,
 * {@link #pollOnce} still calls {@code poll}, which returns nothing for paused
 * partitions, so the consumer stays in its group and the loop stays testable with an
 * injected clock.
 */
public final class RawEventRecorder {

    private static final Logger log = LoggerFactory.getLogger(RawEventRecorder.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final long INITIAL_BACKOFF_NANOS = Duration.ofMillis(500).toNanos();

    private final Consumer<String, byte[]> consumer;
    private final RawBatchWriter writer;
    private final RawRecorderProperties props;
    private final LongSupplier nanoTime;

    private final List<RawRecord> buffer = new ArrayList<>();
    private long bufferBytes;
    private long oldestBufferedNanos;

    private boolean failing;
    private boolean paused;
    private int attempt;
    private long nextAttemptNanos;
    private final AtomicLong lastSuccessNanos = new AtomicLong();

    private final Counter recorded;
    private final Counter objects;
    private final Counter unparsed;
    private final Counter flushFailures;

    public RawEventRecorder(Consumer<String, byte[]> consumer, RawBatchWriter writer,
                            RawRecorderProperties props, MeterRegistry meters, LongSupplier nanoTime) {
        this.consumer = consumer;
        this.writer = writer;
        this.props = props;
        this.nanoTime = nanoTime;
        this.lastSuccessNanos.set(nanoTime.getAsLong());
        this.recorded = meters.counter("muninn.recorder.records");
        this.objects = meters.counter("muninn.recorder.objects");
        this.unparsed = meters.counter("muninn.recorder.unparsed");
        this.flushFailures = meters.counter("muninn.recorder.flush.failures");
        // The one number that reveals a recorder stuck in retry. It climbs without bound
        // while storage is failing, and should never exceed maxBufferAge by much otherwise.
        Gauge.builder("muninn.recorder.last.flush.age.seconds", this,
                        r -> (r.nanoTime.getAsLong() - r.lastSuccessNanos.get()) / 1e9)
                .register(meters);
        Gauge.builder("muninn.recorder.buffered.records", buffer, List::size).register(meters);
    }

    public void subscribe() {
        consumer.subscribe(props.topics(), new Rebalance());
        log.atInfo().addKeyValue("topics", props.topics()).addKeyValue("bucket", props.bucket())
                .log("Raw event recorder subscribed");
    }

    /**
     * One step of the loop: poll, buffer, and flush when due. Never blocks on storage.
     *
     * @return true while there is buffered data still to be stored
     */
    public boolean pollOnce() {
        if (failing) {
            // Partitions are paused, so this returns nothing. It is called to keep the
            // consumer inside max.poll.interval.ms while storage recovers.
            consumer.poll(Duration.ZERO);
        } else {
            ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, byte[]> r : records) {
                if (buffer.isEmpty()) {
                    oldestBufferedNanos = nanoTime.getAsLong();
                }
                buffer.add(new RawRecord(r.topic(), r.partition(), r.offset(), r.timestamp(), r.key(), r.value()));
                bufferBytes += r.value() == null ? 0 : r.value().length;
            }
        }

        long now = nanoTime.getAsLong();
        if (!buffer.isEmpty() && (failing ? now >= nextAttemptNanos : dueToFlush(now))) {
            flush(now);
        }
        return !buffer.isEmpty();
    }

    private boolean dueToFlush(long now) {
        return bufferBytes >= props.maxBufferBytes()
                || now - oldestBufferedNanos >= props.maxBufferAge().toNanos();
    }

    private void flush(long now) {
        RawBatchWriter.Result result;
        try {
            result = writer.write(buffer);
        } catch (RuntimeException e) {
            enterFailing(now, e);
            return;
        }

        try {
            Map<TopicPartition, OffsetAndMetadata> commit = new HashMap<>();
            // Commit the NEXT offset to read, which is one past the last one stored.
            result.lastOffsetWritten().forEach((tp, last) -> commit.put(tp, new OffsetAndMetadata(last + 1)));
            consumer.commitSync(commit);
        } catch (CommitFailedException e) {
            // The data is stored, but the partitions were reassigned under us. Their new
            // owner will redeliver from the last committed offset and rewrite the same
            // objects, so clearing here loses nothing.
            log.atWarn().setCause(e).log("Recorded batch but could not commit; it will be redelivered");
        } catch (RuntimeException e) {
            // Stored but not committed. Retrying rewrites identical objects and commits.
            enterFailing(now, e);
            return;
        }

        recorded.increment(result.records());
        objects.increment(result.objects());
        unparsed.increment(result.unparsed());
        if (result.unparsed() > 0) {
            log.atWarn().addKeyValue("count", result.unparsed())
                    .log("Recorded values that were not valid JSON, under _unparsed");
        }
        buffer.clear();
        bufferBytes = 0;
        lastSuccessNanos.set(now);
        if (failing) {
            log.atInfo().addKeyValue("attempts", attempt).log("Object storage recovered, recording resumed");
        }
        failing = false;
        attempt = 0;
        if (paused) {
            consumer.resume(consumer.assignment());
            paused = false;
        }
    }

    private void enterFailing(long now, RuntimeException cause) {
        flushFailures.increment();
        attempt++;
        long backoff = Math.min(INITIAL_BACKOFF_NANOS << Math.min(attempt - 1, 20), props.maxBackoff().toNanos());
        nextAttemptNanos = now + backoff;
        failing = true;
        if (!paused) {
            consumer.pause(consumer.assignment());
            paused = true;
        }
        log.atError().setCause(cause)
                .addKeyValue("attempt", attempt)
                .addKeyValue("bufferedRecords", buffer.size())
                .addKeyValue("retryInMs", Duration.ofNanos(backoff).toMillis())
                .log("Could not record raw events; holding them and retrying");
    }

    /**
     * Final flush on shutdown, bounded. If storage is down at shutdown the records stay
     * uncommitted and are redelivered on the next start, so stopping loses nothing.
     */
    public void drain(int maxAttempts) {
        for (int i = 0; i < maxAttempts && !buffer.isEmpty(); i++) {
            long now = nanoTime.getAsLong();
            nextAttemptNanos = now;
            flush(now);
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }

    public void close() {
        consumer.close();
    }

    /** Exposed for tests and health reporting. */
    public boolean isFailing() {
        return failing;
    }

    public int bufferedRecords() {
        return buffer.size();
    }

    /** Runs {@link #pollOnce} until {@code running} reports false, then drains and closes. */
    public void run(java.util.function.BooleanSupplier running) {
        subscribe();
        try {
            while (running.getAsBoolean()) {
                try {
                    pollOnce();
                } catch (WakeupException e) {
                    if (running.getAsBoolean()) {
                        throw e;
                    }
                }
            }
            drain(3);
        } finally {
            close();
        }
    }

    private final class Rebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            if (buffer.isEmpty()) {
                return;
            }
            // One attempt only: revocation is time-limited. Whatever cannot be stored is
            // uncommitted, so the new owner reads it again. Drop only the revoked
            // partitions' records, keeping anything for partitions still held.
            if (!failing) {
                flush(nanoTime.getAsLong());
            }
            if (!buffer.isEmpty()) {
                buffer.removeIf(r -> revoked.contains(new TopicPartition(r.topic(), r.partition())));
                bufferBytes = buffer.stream().mapToLong(r -> r.value() == null ? 0 : r.value().length).sum();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
            if (paused && !assigned.isEmpty()) {
                // Newly assigned partitions must not deliver into a buffer we cannot store.
                consumer.pause(assigned);
            }
        }
    }
}
