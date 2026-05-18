package io.muninn.query;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.muninn.query.backend.FeatureQueryBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Service layer for querying feature data.
 *
 * <p>Delegates the actual storage read to whichever {@link FeatureQueryBackend}
 * Spring wired in — DuckDB by default, Trino under
 * {@code muninn.query.backend=trino}. The controller and this service depend
 * only on the abstraction; ArchUnit enforces that the {@code io.muninn.query.*}
 * surface (excluding the {@code backend} sub-package) never imports a
 * concrete backend.</p>
 */
@Service
public class FeatureQueryService {

    private static final Logger log = LoggerFactory.getLogger(FeatureQueryService.class);

    private final FeatureQueryBackend backend;
    private final Counter queryCounter;
    private final Timer queryLatency;

    public FeatureQueryService(FeatureQueryBackend backend, MeterRegistry meterRegistry) {
        this.backend = backend;

        // The backend tag distinguishes DuckDB and Trino in dashboards so a
        // mixed deployment (e.g., during migration) is observable per-engine.
        this.queryCounter = Counter.builder("muninn.query.requests")
                .tag("endpoint", "features")
                .tag("status", "ok")
                .tag("backend", backend.backendId())
                .register(meterRegistry);

        this.queryLatency = Timer.builder("muninn.query.latency")
                .tag("endpoint", "features")
                .tag("backend", backend.backendId())
                .register(meterRegistry);

        log.atInfo()
                .addKeyValue("backend", backend.backendId())
                .log("Feature query service ready");
    }

    public List<Map<String, Object>> queryFeature(
            String featureName,
            String instrument,
            Instant from,
            Instant to
    ) {
        return queryLatency.record(() -> {
            queryCounter.increment();

            log.atDebug()
                    .addKeyValue("feature", featureName)
                    .addKeyValue("instrument", instrument)
                    .addKeyValue("from", from)
                    .addKeyValue("to", to)
                    .addKeyValue("backend", backend.backendId())
                    .log("Querying feature time-series");

            return backend.queryFeatureTimeSeries(featureName, instrument, from, to);
        });
    }
}
