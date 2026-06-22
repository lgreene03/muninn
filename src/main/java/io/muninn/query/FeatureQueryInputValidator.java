package io.muninn.query;

import java.util.regex.Pattern;

/**
 * Strict allowlist validation for feature-query inputs at the API boundary.
 *
 * <p>{@code featureName} and {@code instrument} flow into storage-backend SQL
 * builders that cannot parameter-bind an identifier (DuckDB's
 * {@code read_parquet} path argument, a Trino table identifier). Both inputs
 * MUST therefore be constrained to a strict character allowlist <em>before</em>
 * they reach any backend, so that quotes, whitespace, parentheses, glob
 * characters and path-traversal sequences can never be interpolated into a
 * query. This is the single source of truth for those patterns.</p>
 */
public final class FeatureQueryInputValidator {

    /** Letters, digits, dot, underscore, hyphen — e.g. {@code vwap.1m}. */
    static final Pattern FEATURE_NAME = Pattern.compile("^[A-Za-z0-9._-]+$");

    /** Letters, digits, hyphen — e.g. {@code BTC-USDT}. */
    static final Pattern INSTRUMENT = Pattern.compile("^[A-Za-z0-9-]+$");

    private FeatureQueryInputValidator() {
    }

    /**
     * @throws IllegalArgumentException if {@code featureName} is null/blank or
     *         contains any character outside {@code [A-Za-z0-9._-]}.
     */
    public static String requireValidFeatureName(String featureName) {
        if (featureName == null || !FEATURE_NAME.matcher(featureName).matches()) {
            throw new IllegalArgumentException(
                    "'featureName' must match " + FEATURE_NAME.pattern());
        }
        return featureName;
    }

    /**
     * @throws IllegalArgumentException if {@code instrument} is null/blank or
     *         contains any character outside {@code [A-Za-z0-9-]}.
     */
    public static String requireValidInstrument(String instrument) {
        if (instrument == null || !INSTRUMENT.matcher(instrument).matches()) {
            throw new IllegalArgumentException(
                    "'instrument' must match " + INSTRUMENT.pattern());
        }
        return instrument;
    }
}
