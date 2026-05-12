package io.muninn.replay;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayJobRegistryTest {

    @Test
    void putAndFindRoundTrips() {
        ReplayJobRegistry registry = new ReplayJobRegistry();
        ReplayJob job = sample();

        registry.put(job);

        assertThat(registry.find(job.jobId())).contains(job);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void findReturnsEmptyForUnknownJob() {
        ReplayJobRegistry registry = new ReplayJobRegistry();
        assertThat(registry.find(UUID.randomUUID())).isEmpty();
    }

    @Test
    void putOverwritesPreviousStateForSameJobId() {
        ReplayJobRegistry registry = new ReplayJobRegistry();
        ReplayJob job = sample();
        registry.put(job);

        ReplayJob running = job.started(Instant.parse("2026-05-11T10:01:00Z"));
        registry.put(running);

        assertThat(registry.find(job.jobId()))
                .map(ReplayJob::status)
                .contains(ReplayJobStatus.RUNNING);
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void allReturnsAllJobs() {
        ReplayJobRegistry registry = new ReplayJobRegistry();
        registry.put(sample());
        registry.put(sample());

        assertThat(registry.all()).hasSize(2);
    }

    private ReplayJob sample() {
        return new ReplayJob(
                UUID.randomUUID(),
                List.of("events.trade"),
                Instant.parse("2026-05-10T14:00:00Z"),
                Instant.parse("2026-05-10T15:00:00Z"),
                "v1",
                "features.v1.replay",
                ReplayJobStatus.PENDING,
                0L,
                Instant.parse("2026-05-11T10:00:00Z"),
                null, null, null, null
        );
    }
}
