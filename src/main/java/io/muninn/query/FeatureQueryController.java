package io.muninn.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * Read-only HTTP endpoint for querying computed features.
 *
 * <p>Serves feature time-series data from the Parquet warehouse via DuckDB.
 * This is the external-facing read path — no mutations, no side effects.</p>
 */
@RestController
@RequestMapping("/api/v1/features")
public class FeatureQueryController {

    private static final Logger log = LoggerFactory.getLogger(FeatureQueryController.class);

    private final FeatureQueryService queryService;

    public FeatureQueryController(FeatureQueryService queryService) {
        this.queryService = queryService;
    }

    /**
     * Query a feature time series.
     *
     * @param featureName the feature name (e.g., "vwap")
     * @param instrument  the instrument symbol (e.g., "BTC-USDT")
     * @param from        the start of the time range (inclusive)
     * @param to          the end of the time range (exclusive)
     * @return the feature data points
     */
    @GetMapping("/{featureName}")
    public ResponseEntity<Map<String, Object>> queryFeature(
            @PathVariable String featureName,
            @RequestParam String instrument,
            @RequestParam Instant from,
            @RequestParam Instant to
    ) {
        log.atInfo()
                .addKeyValue("feature", featureName)
                .addKeyValue("instrument", instrument)
                .addKeyValue("from", from)
                .addKeyValue("to", to)
                .log("Feature query received");

        if (from.isAfter(to)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "error",
                    "message", "'from' must be before 'to'"
            ));
        }

        var result = queryService.queryFeature(featureName, instrument, from, to);

        return ResponseEntity.ok(Map.of(
                "feature", featureName + ".1m",
                "version", "v1",
                "instrument", instrument,
                "from", from.toString(),
                "to", to.toString(),
                "points", result
        ));
    }
}
