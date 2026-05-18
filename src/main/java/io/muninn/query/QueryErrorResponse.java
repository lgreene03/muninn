package io.muninn.query;

import java.time.Instant;

/**
 * Structured error response for the Query API.
 *
 * <p>Returned by {@link QueryExceptionHandler} for all handled error conditions.</p>
 */
public record QueryErrorResponse(
        String status,
        String message,
        String path,
        Instant timestamp
) {}
