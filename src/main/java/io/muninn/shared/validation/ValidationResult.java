package io.muninn.shared.validation;

import java.util.List;

/**
 * Result of event validation. Sealed to {@link Valid} and {@link Invalid}.
 *
 * <p>Pattern-match on the result to handle both cases:</p>
 * <pre>{@code
 * switch (result) {
 *     case ValidationResult.Valid v -> publish(event);
 *     case ValidationResult.Invalid inv -> deadLetter(event, inv.reasons());
 * }
 * }</pre>
 */
public sealed interface ValidationResult {

    /** The event passed all validation checks. */
    record Valid() implements ValidationResult {}

    /** The event failed one or more validation checks. */
    record Invalid(List<String> reasons) implements ValidationResult {
        public Invalid {
            if (reasons == null || reasons.isEmpty()) {
                throw new IllegalArgumentException("Invalid result must have at least one reason");
            }
            reasons = List.copyOf(reasons);
        }
    }

    /** Convenience: is this result valid? */
    default boolean isValid() {
        return this instanceof Valid;
    }
}
