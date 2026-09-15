/**
 * Microstructure feature computers — pure functions over order-book and trade state.
 *
 * <p>Each computer in this package (order-book imbalance, micro-price, VPIN) follows
 * the same discipline as {@link io.muninn.feature.compute.VwapComputer}: {@link java.math.BigDecimal}
 * arithmetic with an explicit {@link java.math.MathContext}, no mutable instance state,
 * no wall-clock reads, no IO. Where a computer needs state that spans multiple windows
 * (VPIN's volume buckets do not align to time windows), that state is an explicit,
 * immutable value passed in and returned — never held on the instance — so the computer
 * itself remains a pure function of its inputs (see DETERMINISTIC_REPLAY.md
 * §Anti-Patterns and {@link io.muninn.feature.FeatureComputer}).</p>
 */
package io.muninn.feature.microstructure;
