/**
 * Reference-data types for instruments and exchanges.
 *
 * <p>These types are immutable records used across all modules. They carry no behavior
 * beyond construction-time validation. Instrument and Exchange data is seeded in PostgreSQL
 * via Flyway and loaded at startup.</p>
 */
package io.muninn.shared.instrument;
