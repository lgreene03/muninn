package io.muninn.streaming;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Server-Sent Events endpoint that streams {@link io.muninn.shared.event.FeatureComputedEvent}s to
 * clients as the feature engine produces them — a push alternative to polling
 * {@code GET /api/v1/features/{featureName}}.
 *
 * <p>{@code GET /api/v1/features/stream} opens a {@code text/event-stream}. Each computed event is
 * delivered as an SSE event named {@code feature} whose {@code data} is the JSON event. An optional
 * {@code feature} query parameter restricts the stream to a single feature name
 * (e.g. {@code ?feature=vwap.1m}).</p>
 */
@RestController
@RequestMapping("/api/v1/features")
public class FeatureStreamController {

    private final FeatureStreamBroker broker;

    public FeatureStreamController(FeatureStreamBroker broker) {
        this.broker = broker;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam(required = false) String feature) {
        return broker.register(feature);
    }
}
