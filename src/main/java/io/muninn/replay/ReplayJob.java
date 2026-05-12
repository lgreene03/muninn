package io.muninn.replay;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A scheduled or ad-hoc replay of a portion of the event log through the feature engine.
 *
 * <p>Submitted by callers, advanced through {@link ReplayJobStatus} by the {@link ReplayJobRunner},
 * and persisted (today) in an in-memory {@link ReplayJobRegistry}.</p>
 *
 * <p>See {@code docs/steering/DOMAIN_MODEL.md} §ReplayJob.</p>
 *
 * @param jobId          unique identifier (UUIDv7 recommended)
 * @param topics         input topics to read from
 * @param from           inclusive event-time start of the replay range
 * @param to             exclusive event-time end of the replay range
 * @param featureVersion which feature version's outputs to produce
 * @param outputSink     opaque sink descriptor (a Kafka topic name in MVP)
 * @param status         current lifecycle state
 * @param eventsReplayed count of events processed so far (best-effort)
 * @param submittedAt    when the job was created
 * @param startedAt      when the runner picked it up; null while {@code PENDING}
 * @param completedAt    when the runner finished; null while not yet terminal
 * @param elapsed        wall-clock duration of the run; null while not yet terminal
 * @param error          terminal error message; null unless {@code FAILED}
 */
public record ReplayJob(
        UUID jobId,
        List<String> topics,
        Instant from,
        Instant to,
        String featureVersion,
        String outputSink,
        ReplayJobStatus status,
        long eventsReplayed,
        Instant submittedAt,
        Instant startedAt,
        Instant completedAt,
        Duration elapsed,
        String error
) {

    public ReplayJob {
        if (jobId == null) throw new IllegalArgumentException("jobId is required");
        if (topics == null || topics.isEmpty()) throw new IllegalArgumentException("topics is required");
        if (from == null) throw new IllegalArgumentException("from is required");
        if (to == null) throw new IllegalArgumentException("to is required");
        if (!from.isBefore(to)) throw new IllegalArgumentException("from must be before to");
        if (featureVersion == null || featureVersion.isBlank()) throw new IllegalArgumentException("featureVersion is required");
        if (outputSink == null || outputSink.isBlank()) throw new IllegalArgumentException("outputSink is required");
        if (status == null) throw new IllegalArgumentException("status is required");
        if (submittedAt == null) throw new IllegalArgumentException("submittedAt is required");
        topics = List.copyOf(topics);
    }

    public ReplayJob withStatus(ReplayJobStatus newStatus) {
        return new ReplayJob(jobId, topics, from, to, featureVersion, outputSink,
                newStatus, eventsReplayed, submittedAt, startedAt, completedAt, elapsed, error);
    }

    public ReplayJob started(Instant when) {
        return new ReplayJob(jobId, topics, from, to, featureVersion, outputSink,
                ReplayJobStatus.RUNNING, eventsReplayed, submittedAt, when, completedAt, elapsed, error);
    }

    public ReplayJob progress(long events) {
        return new ReplayJob(jobId, topics, from, to, featureVersion, outputSink,
                status, events, submittedAt, startedAt, completedAt, elapsed, error);
    }

    public ReplayJob completed(Instant when, long events, Duration totalElapsed) {
        return new ReplayJob(jobId, topics, from, to, featureVersion, outputSink,
                ReplayJobStatus.COMPLETED, events, submittedAt, startedAt, when, totalElapsed, error);
    }

    public ReplayJob failed(Instant when, String errorMessage, Duration totalElapsed) {
        return new ReplayJob(jobId, topics, from, to, featureVersion, outputSink,
                ReplayJobStatus.FAILED, eventsReplayed, submittedAt, startedAt, when, totalElapsed, errorMessage);
    }

    public boolean isTerminal() {
        return status == ReplayJobStatus.COMPLETED || status == ReplayJobStatus.FAILED;
    }
}
