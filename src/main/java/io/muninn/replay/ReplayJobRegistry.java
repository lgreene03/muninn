package io.muninn.replay;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory registry of {@link ReplayJob}s.
 *
 * <p>MVP storage. Phase 4 keeps job state in-process for simplicity; persisting
 * to PostgreSQL (along with the existing {@code replay_jobs} migration) is a
 * Phase 5 concern, when an operator may want to query historical jobs across
 * restarts. See {@code docs/steering/DOMAIN_MODEL.md} §ReplayJob.</p>
 *
 * <p>Operations are thread-safe via a backing {@link ConcurrentHashMap}.</p>
 */
@Component
public class ReplayJobRegistry {

    private final ConcurrentMap<UUID, ReplayJob> jobs = new ConcurrentHashMap<>();

    public void put(ReplayJob job) {
        jobs.put(job.jobId(), job);
    }

    public Optional<ReplayJob> find(UUID jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    public Collection<ReplayJob> all() {
        return jobs.values();
    }

    public int size() {
        return jobs.size();
    }
}
