/**
 * Feature engine windowing and orchestration.
 *
 * <p>Contains the core streaming primitives: tumbling window assignment,
 * watermark tracking, and window management. These components are pure
 * and deterministic — they do not read the wall clock or perform IO.</p>
 */
package io.muninn.feature.engine;
