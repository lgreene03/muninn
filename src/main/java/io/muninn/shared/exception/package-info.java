/**
 * Typed domain exception hierarchy for Muninn.
 *
 * <p>All domain exceptions extend {@link io.muninn.shared.exception.MuninnException}, which is abstract
 * to force callers to use specific subtypes. Subtypes represent failure domains:
 * validation, ingestion, storage, and replay.</p>
 *
 * <p>Checked exceptions are not used. All exceptions in this hierarchy are {@link RuntimeException}s,
 * wrapped at service boundaries with structured context.</p>
 */
package io.muninn.shared.exception;
