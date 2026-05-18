package io.muninn.query;

import io.muninn.shared.exception.StorageException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/**
 * Global exception handler for the Query API.
 *
 * <p>Translates typed exceptions into structured {@link QueryErrorResponse} bodies.
 * Internal error details are never leaked to the caller.</p>
 */
@RestControllerAdvice(basePackages = "io.muninn.query")
public class QueryExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(QueryExceptionHandler.class);

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<QueryErrorResponse> handleIllegalArgument(
            IllegalArgumentException ex, HttpServletRequest request) {
        log.atWarn()
                .addKeyValue("path", request.getRequestURI())
                .addKeyValue("error", ex.getMessage())
                .log("Query API bad request");

        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new QueryErrorResponse(
                        "error",
                        ex.getMessage(),
                        request.getRequestURI(),
                        Instant.now()
                ));
    }

    @ExceptionHandler(StorageException.class)
    public ResponseEntity<QueryErrorResponse> handleStorageException(
            StorageException ex, HttpServletRequest request) {
        log.atError()
                .addKeyValue("path", request.getRequestURI())
                .addKeyValue("error", ex.getMessage())
                .log("Query engine failure");

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new QueryErrorResponse(
                        "error",
                        "Query engine temporarily unavailable",
                        request.getRequestURI(),
                        Instant.now()
                ));
    }
}
