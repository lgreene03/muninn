package io.muninn.shared.instrument;

import java.time.ZoneId;

/**
 * A data source for market events.
 *
 * @param id          stable identifier (e.g., {@code "binance"})
 * @param displayName human-readable name (e.g., {@code "Binance Spot"})
 * @param timezone    the exchange's canonical timezone
 */
public record Exchange(
        String id,
        String displayName,
        ZoneId timezone
) implements java.io.Serializable {

    public Exchange {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("Exchange id is required");
        if (displayName == null || displayName.isBlank()) throw new IllegalArgumentException("Exchange displayName is required");
        if (timezone == null) throw new IllegalArgumentException("Exchange timezone is required");
    }
}
