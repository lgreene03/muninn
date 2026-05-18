package io.muninn.query;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Features", description = "Feature time-series queries")
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
    @Operation(summary = "Query a feature time series")
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Feature time-series data"),
        @ApiResponse(responseCode = "400", description = "Invalid time range")
    })
    public ResponseEntity<Object> queryFeature(
            @Parameter(description = "Feature name, e.g. vwap") @PathVariable String featureName,
            @Parameter(description = "Instrument symbol, e.g. BTC-USDT") @RequestParam String instrument,
            @Parameter(description = "Start of time range (ISO-8601 instant, inclusive)", schema = @Schema(type = "string", format = "date-time")) @RequestParam Instant from,
            @Parameter(description = "End of time range (ISO-8601 instant, exclusive)", schema = @Schema(type = "string", format = "date-time")) @RequestParam Instant to
    ) {
        log.atInfo()
                .addKeyValue("feature", featureName)
                .addKeyValue("instrument", instrument)
                .addKeyValue("from", from)
                .addKeyValue("to", to)
                .log("Feature query received");

        if (from.isAfter(to)) {
            return ResponseEntity.badRequest().body(new QueryErrorResponse(
                    "error",
                    "'from' must be before 'to'",
                    "/api/v1/features/" + featureName,
                    Instant.now()
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
