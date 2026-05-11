/**
 * Feature engine checkpoint management.
 *
 * <p>Handles serialization, persistence (MinIO), and restoration of feature
 * engine state. Checkpoints enable the engine to resume from a known watermark
 * after restart without replaying from {@code t=0}.</p>
 */
package io.muninn.feature.checkpoint;
