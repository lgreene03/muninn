package io.muninn.storage.raw;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Configuration for the raw-event recorder, bound from {@code muninn.recorder.*}.
 *
 * @param enabled        whether the recorder runs at all
 * @param topics         raw topics to record
 * @param groupId        consumer group; its committed offsets are the recorder's progress
 * @param bucket         destination bucket
 * @param maxBufferBytes flush once this much value data is buffered
 * @param maxBufferAge   flush once the oldest buffered record is this old, so a quiet
 *                       stream still reaches storage
 * @param maxBackoff     ceiling on the retry delay while storage is failing. Kept well
 *                       under the consumer's {@code max.poll.interval.ms} (5 minutes by
 *                       default), although the recorder keeps polling while it waits
 */
@ConfigurationProperties(prefix = "muninn.recorder")
public record RawRecorderProperties(
        boolean enabled,
        List<String> topics,
        String groupId,
        String bucket,
        long maxBufferBytes,
        Duration maxBufferAge,
        Duration maxBackoff
) {
    public RawRecorderProperties {
        if (topics == null || topics.isEmpty()) topics = List.of("events.trade", "events.book.snapshot");
        if (groupId == null || groupId.isBlank()) groupId = "muninn-raw-recorder";
        if (bucket == null || bucket.isBlank()) bucket = "muninn-raw";
        if (maxBufferBytes <= 0) maxBufferBytes = 8L * 1024 * 1024;
        if (maxBufferAge == null || maxBufferAge.isZero() || maxBufferAge.isNegative()) maxBufferAge = Duration.ofSeconds(60);
        if (maxBackoff == null || maxBackoff.isZero() || maxBackoff.isNegative()) maxBackoff = Duration.ofSeconds(30);
        topics = List.copyOf(topics);
    }
}
