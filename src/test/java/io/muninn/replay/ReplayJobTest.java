package io.muninn.replay;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplayJobTest {

    private static final UUID JOB_ID = UUID.fromString("01900000-0000-7000-8000-000000000001");
    private static final Instant FROM = Instant.parse("2026-05-10T14:00:00Z");
    private static final Instant TO = Instant.parse("2026-05-10T15:00:00Z");
    private static final Instant SUBMITTED = Instant.parse("2026-05-11T10:00:00Z");

    @Test
    void newJob_isPending() {
        ReplayJob job = newPending();

        assertThat(job.status()).isEqualTo(ReplayJobStatus.PENDING);
        assertThat(job.startedAt()).isNull();
        assertThat(job.completedAt()).isNull();
        assertThat(job.error()).isNull();
        assertThat(job.eventsReplayed()).isZero();
        assertThat(job.isTerminal()).isFalse();
    }

    @Test
    void started_transitionsToRunning() {
        Instant when = Instant.parse("2026-05-11T10:01:00Z");
        ReplayJob job = newPending().started(when);

        assertThat(job.status()).isEqualTo(ReplayJobStatus.RUNNING);
        assertThat(job.startedAt()).isEqualTo(when);
        assertThat(job.isTerminal()).isFalse();
    }

    @Test
    void completed_transitionsAndRecordsElapsed() {
        Instant started = Instant.parse("2026-05-11T10:01:00Z");
        Instant finished = Instant.parse("2026-05-11T10:02:30Z");

        ReplayJob job = newPending().started(started).completed(finished, 1500, Duration.between(started, finished));

        assertThat(job.status()).isEqualTo(ReplayJobStatus.COMPLETED);
        assertThat(job.completedAt()).isEqualTo(finished);
        assertThat(job.eventsReplayed()).isEqualTo(1500);
        assertThat(job.elapsed()).isEqualTo(Duration.ofSeconds(90));
        assertThat(job.isTerminal()).isTrue();
    }

    @Test
    void failed_recordsErrorAndIsTerminal() {
        Instant started = Instant.parse("2026-05-11T10:01:00Z");
        Instant finished = Instant.parse("2026-05-11T10:01:05Z");

        ReplayJob job = newPending().started(started).failed(finished, "broker unreachable", Duration.ofSeconds(5));

        assertThat(job.status()).isEqualTo(ReplayJobStatus.FAILED);
        assertThat(job.error()).isEqualTo("broker unreachable");
        assertThat(job.isTerminal()).isTrue();
    }

    @Test
    void rejectsRangeWhereFromIsNotBeforeTo() {
        assertThatThrownBy(() -> new ReplayJob(
                JOB_ID, List.of("events.trade"), TO, FROM,
                "v1", "features.v1.replay", ReplayJobStatus.PENDING,
                0L, SUBMITTED, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("from must be before to");
    }

    @Test
    void rejectsEmptyTopics() {
        assertThatThrownBy(() -> new ReplayJob(
                JOB_ID, List.of(), FROM, TO,
                "v1", "features.v1.replay", ReplayJobStatus.PENDING,
                0L, SUBMITTED, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topics");
    }

    private ReplayJob newPending() {
        return new ReplayJob(
                JOB_ID,
                List.of("events.trade"),
                FROM,
                TO,
                "v1",
                "features.v1.replay",
                ReplayJobStatus.PENDING,
                0L,
                SUBMITTED,
                null,
                null,
                null,
                null
        );
    }
}
