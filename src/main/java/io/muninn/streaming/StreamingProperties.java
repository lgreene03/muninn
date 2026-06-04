package io.muninn.streaming;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Configuration for the live feature-streaming (SSE) endpoint, bound from {@code muninn.streaming.*}.
 *
 * <p>See {@code docs/adr/0009-streaming-features-sse.md}.</p>
 *
 * @param enabled           whether the background Kafka tail consumer runs. When {@code false} the
 *                          HTTP endpoint still accepts connections (and emits keepalives) but no
 *                          feature events are broadcast.
 * @param topicPattern      Java regex matching the feature output topics to tail (default
 *                          {@code features\..*}, e.g. {@code features.vwap.1m.v1}).
 * @param pollTimeout       Kafka {@code poll()} timeout for the tail loop.
 * @param emitterTimeout    server-side SSE emitter timeout; {@link Duration#ZERO} means no timeout
 *                          (rely on keepalives to prune dead connections).
 * @param keepaliveInterval cadence at which a comment frame is sent to every open connection so
 *                          idle proxies stay open and dead sockets are detected and pruned.
 */
@ConfigurationProperties(prefix = "muninn.streaming")
public record StreamingProperties(
        boolean enabled,
        String topicPattern,
        Duration pollTimeout,
        Duration emitterTimeout,
        Duration keepaliveInterval
) {
    public StreamingProperties {
        if (topicPattern == null || topicPattern.isBlank()) {
            topicPattern = "features\\..*";
        }
        if (pollTimeout == null || pollTimeout.isZero() || pollTimeout.isNegative()) {
            pollTimeout = Duration.ofMillis(500);
        }
        if (emitterTimeout == null || emitterTimeout.isNegative()) {
            emitterTimeout = Duration.ZERO;
        }
        if (keepaliveInterval == null || keepaliveInterval.isZero() || keepaliveInterval.isNegative()) {
            keepaliveInterval = Duration.ofSeconds(15);
        }
    }
}
