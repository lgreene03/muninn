/**
 * Time utilities for Muninn.
 *
 * <p>Contains UUIDv7 generation (time-ordered identifiers per RFC 9562) and any
 * future watermark or event-time utilities. All time handling in Muninn uses
 * {@link java.time.Instant} for absolute time and {@link java.time.Duration} for
 * elapsed time — never raw {@code long} without a unit suffix.</p>
 */
package io.muninn.shared.time;
