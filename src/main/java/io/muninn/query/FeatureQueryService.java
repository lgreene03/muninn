package io.muninn.query;

import io.muninn.storage.DuckDbQueryService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Service layer for querying feature data from DuckDB over Parquet.
 *
 * <p>In the MVP, this queries the DuckDB in-memory instance. When the Parquet
 * writer (Step 7) is complete, this will query Parquet files on MinIO via the
 * DuckDB httpfs extension.</p>
 *
 * <p>For Phase 3 MVP, this also supports a "recent" mode that reads from the
 * Kafka consumer offset for near-real-time data not yet rolled to Parquet.</p>
 */
@Service
public class FeatureQueryService {

    private static final Logger log = LoggerFactory.getLogger(FeatureQueryService.class);

    private final DuckDbQueryService duckDbQueryService;
    private final Counter queryCounter;
    private final Timer queryLatency;

    public FeatureQueryService(DuckDbQueryService duckDbQueryService, MeterRegistry meterRegistry) {
        this.duckDbQueryService = duckDbQueryService;

        this.queryCounter = Counter.builder("muninn.query.requests")
                .tag("endpoint", "features")
                .tag("status", "ok")
                .register(meterRegistry);

        this.queryLatency = Timer.builder("muninn.query.latency")
                .tag("endpoint", "features")
                .register(meterRegistry);
    }

    /**
     * Query feature data points within a time range.
     *
     * <p>For the MVP, returns data from DuckDB. When no Parquet data is available
     * (e.g., during initial development), returns an empty list with a log message.</p>
     *
     * @param featureName the feature name
     * @param instrument  the instrument symbol
     * @param from        inclusive start time
     * @param to          exclusive end time
     * @return list of data points as maps
     */
    public List<Map<String, Object>> queryFeature(String featureName, String instrument, Instant from, Instant to) {
        return queryLatency.record(() -> {
            queryCounter.increment();

            log.atDebug()
                    .addKeyValue("feature", featureName)
                    .addKeyValue("instrument", instrument)
                    .addKeyValue("from", from)
                    .addKeyValue("to", to)
                    .log("Querying feature data from DuckDB");

            // In MVP: DuckDB over Parquet. When Parquet writer is wired,
            // this will query: read_parquet('s3://muninn-warehouse/features.{featureName}.v1/...')
            // For now, return empty list with structured log
            String sql = """
                    SELECT window_start, window_end, vwap_value, event_count
                    FROM read_parquet('s3://muninn-warehouse/features.%s.v1/instrument=%s/**/*.parquet',
                                      hive_partitioning=true)
                    WHERE window_start >= '%s' AND window_end <= '%s'
                    ORDER BY window_start
                    """.formatted(featureName, instrument, from, to);

            try {
                return duckDbQueryService.query(sql);
            } catch (Exception e) {
                // Expected during early development when Parquet files don't exist yet
                log.atDebug()
                        .addKeyValue("error", e.getMessage())
                        .log("Feature query returned no data (Parquet warehouse may not be populated yet)");
                return List.of();
            }
        });
    }
}
