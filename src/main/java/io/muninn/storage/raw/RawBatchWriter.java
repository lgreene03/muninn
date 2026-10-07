package io.muninn.storage.raw;

import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * Writes a batch of raw records to object storage, one gzipped JSON-lines object per
 * (topic, partition, source, instrument, hour).
 *
 * <p>All-or-nothing from the caller's point of view: {@link #write} either stores every
 * group and returns the highest offset written per partition, or throws. It never
 * reports success for a partial write, because the caller commits Kafka offsets on the
 * strength of the return value. If a later group fails after earlier ones succeeded,
 * the exception propagates, nothing is committed, and the redelivered batch rewrites
 * the earlier objects under the same deterministic keys.
 */
public final class RawBatchWriter {

    private final ObjectPutter putter;
    private final RawRecordEncoder encoder = new RawRecordEncoder();

    public RawBatchWriter(ObjectPutter putter) {
        this.putter = putter;
    }

    /** What a successful write stored. */
    public record Result(Map<TopicPartition, Long> lastOffsetWritten, int objects, int records, int unparsed) {}

    private record Group(String topic, int partition, String source, String instrument, long hourStart) {}

    private record Line(long offset, byte[] bytes) {}

    public Result write(List<RawRecord> records) {
        Map<Group, List<Line>> groups = new LinkedHashMap<>();
        Map<TopicPartition, Long> lastOffset = new HashMap<>();
        int unparsed = 0;

        for (RawRecord r : records) {
            RawRecordEncoder.Encoded e = encoder.encode(r);
            if (!e.parsed() && r.value() != null) {
                unparsed++;
            }
            Group g = new Group(r.topic(), r.partition(), e.source(), e.instrument(),
                    RawObjectKeys.hourStart(r.timestamp()));
            groups.computeIfAbsent(g, k -> new ArrayList<>()).add(new Line(r.offset(), e.line()));
            lastOffset.merge(new TopicPartition(r.topic(), r.partition()), r.offset(), Math::max);
        }

        for (Map.Entry<Group, List<Line>> entry : groups.entrySet()) {
            Group g = entry.getKey();
            List<Line> lines = entry.getValue();
            // Within a partition Kafka delivers in offset order already; sorting makes the
            // file content, and so the object, independent of how the batch was assembled.
            lines.sort(Comparator.comparingLong(Line::offset));
            String key = RawObjectKeys.key(g.topic(), g.source(), g.instrument(), g.partition(),
                    g.hourStart(), lines.getFirst().offset(), lines.getLast().offset());
            putter.put(key, gzip(lines));
        }

        return new Result(Map.copyOf(lastOffset), groups.size(), records.size(), unparsed);
    }

    private static byte[] gzip(List<Line> lines) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bytes)) {
            for (Line line : lines) {
                gz.write(line.bytes());
            }
        } catch (IOException e) {
            // In-memory streams do not fail; this is not a storage error.
            throw new UncheckedIOException(e);
        }
        return bytes.toByteArray();
    }
}
