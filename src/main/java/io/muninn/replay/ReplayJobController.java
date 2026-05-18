package io.muninn.replay;

import io.muninn.shared.time.UUIDv7;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * HTTP API for {@link ReplayJob} submission and inspection.
 *
 * <p>Submission is fire-and-forget: the runner picks the job up asynchronously
 * and the caller polls the status endpoint. There is no streaming progress API
 * in MVP — a follow-up could add SSE if it proves useful.</p>
 */
@RestController
@RequestMapping("/api/v1/replay/jobs")
@Tag(name = "Replay Jobs", description = "Replay job submission and status")
public class ReplayJobController {

    private final ReplayJobRunner runner;
    private final ReplayJobRegistry registry;

    public ReplayJobController(ReplayJobRunner runner, ReplayJobRegistry registry) {
        this.runner = runner;
        this.registry = registry;
    }

    /**
     * Submit a new replay job. The request body is the user-supplied portion of
     * a {@link ReplayJob}; the server fills in jobId, submittedAt, and the
     * initial PENDING status.
     */
    @PostMapping
    @Operation(summary = "Submit a replay job")
    public ResponseEntity<ReplayJob> submit(@RequestBody SubmitRequest request) {
        if (request.from() == null || request.to() == null) {
            return ResponseEntity.badRequest().build();
        }
        UUID jobId = UUIDv7.generate();
        List<String> topics = request.topics() != null && !request.topics().isEmpty()
                ? request.topics()
                : List.of("events.trade");
        String featureVersion = request.featureVersion() != null && !request.featureVersion().isBlank()
                ? request.featureVersion()
                : "v1";
        String outputSink = "features." + featureVersion + ReplayJobRunner.REPLAY_TOPIC_SUFFIX;

        ReplayJob job = new ReplayJob(
                jobId,
                topics,
                request.from(),
                request.to(),
                featureVersion,
                outputSink,
                ReplayJobStatus.PENDING,
                0L,
                Instant.now(),
                null,
                null,
                null,
                null
        );

        runner.submit(job);
        return ResponseEntity
                .created(URI.create("/api/v1/replay/jobs/" + jobId))
                .body(job);
    }

    @GetMapping("/{jobId}")
    @Operation(summary = "Get replay job by ID")
    public ResponseEntity<ReplayJob> get(@PathVariable UUID jobId) {
        return registry.find(jobId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    @Operation(summary = "List all replay jobs")
    public Collection<ReplayJob> list() {
        return registry.all();
    }

    /**
     * Submission payload. Fields that the caller cannot meaningfully provide
     * (jobId, submittedAt, status) are server-supplied.
     */
    public record SubmitRequest(
            List<String> topics,
            Instant from,
            Instant to,
            String featureVersion
    ) { }
}
