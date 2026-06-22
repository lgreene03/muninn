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
import java.util.List;
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
    private final FeatureCatalogService catalogService;

    public FeatureQueryController(FeatureQueryService queryService, FeatureCatalogService catalogService) {
        this.queryService = queryService;
        this.catalogService = catalogService;
    }

    /**
     * List all registered feature definitions.
     *
     * @return the catalog of active feature definitions
     */
    @GetMapping
    @Operation(summary = "List all registered feature definitions")
    @ApiResponses(@ApiResponse(responseCode = "200", description = "Registered feature definitions"))
    public List<FeatureDefinitionSummary> listFeatures() {
        return catalogService.listDefinitions();
    }

    /**
     * Query a feature time series.
     *
     * @param featureName the feature name (e.g., "vwap")
     * @param instrument  the instrument symbol (e.g., "BTC-USDT")
     * @param start       the start of the time range (inclusive)
     * @param end         the end of the time range (exclusive)
     * @param limit       optional cap on the number of returned rows
     * @return the feature time-series rows under a {@code "values"} envelope
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
            @Parameter(description = "Start of time range (ISO-8601 instant, inclusive)", schema = @Schema(type = "string", format = "date-time")) @RequestParam Instant start,
            @Parameter(description = "End of time range (ISO-8601 instant, exclusive)", schema = @Schema(type = "string", format = "date-time")) @RequestParam Instant end,
            @Parameter(description = "Maximum number of rows to return") @RequestParam(required = false) Integer limit
    ) {
        log.atInfo()
                .addKeyValue("feature", featureName)
                .addKeyValue("instrument", instrument)
                .addKeyValue("start", start)
                .addKeyValue("end", end)
                .log("Feature query received");

        // Strict allowlist validation at the boundary: featureName and instrument
        // are interpolated into backend SQL identifiers that cannot be parameter-
        // bound (DuckDB read_parquet path, Trino table name). Reject anything
        // outside the allowlist with 400 BEFORE it can reach any SQL builder.
        // A non-matching value throws IllegalArgumentException, mapped to 400 by
        // QueryExceptionHandler.
        FeatureQueryInputValidator.requireValidFeatureName(featureName);
        FeatureQueryInputValidator.requireValidInstrument(instrument);

        if (start.isAfter(end)) {
            return ResponseEntity.badRequest().body(new QueryErrorResponse(
                    "error",
                    "'start' must be before 'end'",
                    "/api/v1/features/" + featureName,
                    Instant.now()
            ));
        }

        var result = queryService.queryFeature(featureName, instrument, start, end);
        if (limit != null && limit >= 0 && limit < result.size()) {
            result = result.subList(0, limit);
        }

        return ResponseEntity.ok(Map.of("values", result));
    }
}
