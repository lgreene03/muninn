/**
 * Event validation for Muninn's ingestion boundary.
 *
 * <p>{@link io.muninn.shared.validation.EventValidator} enforces the rules from
 * EVENT_SCHEMA_STRATEGY.md: required fields, time-range checks, clock-skew tolerance,
 * numeric bounds, and reference-data resolution. Validation results are returned as
 * a sealed {@link io.muninn.shared.validation.ValidationResult} — never thrown as exceptions
 * in the validation path itself.</p>
 */
package io.muninn.shared.validation;
