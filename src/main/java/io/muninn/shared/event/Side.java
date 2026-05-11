package io.muninn.shared.event;

/**
 * The side of a trade as reported by the exchange.
 *
 * <p>{@code UNKNOWN} is used when the exchange does not report the aggressor side
 * or when the adapter cannot determine it reliably.</p>
 */
public enum Side {
    BUY,
    SELL,
    UNKNOWN
}
