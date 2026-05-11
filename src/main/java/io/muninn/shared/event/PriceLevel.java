package io.muninn.shared.event;

import java.math.BigDecimal;

/**
 * A single level in an order book — a price and its associated size.
 *
 * @param price the price at this level
 * @param size  the aggregate size available at this price
 */
public record PriceLevel(
        BigDecimal price,
        BigDecimal size
) {

    public PriceLevel {
        if (price == null) throw new IllegalArgumentException("PriceLevel price is required");
        if (size == null) throw new IllegalArgumentException("PriceLevel size is required");
        if (price.signum() <= 0) throw new IllegalArgumentException("PriceLevel price must be positive");
        if (size.signum() < 0) throw new IllegalArgumentException("PriceLevel size must not be negative");
    }
}
