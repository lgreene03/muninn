package io.muninn.ingestion.adapter;

import io.muninn.shared.exception.IngestionException;

/**
 * Thrown when a Binance WebSocket message cannot be parsed into a canonical event.
 * Carries the raw message for dead-letter routing and debugging.
 */
public class BinanceParseException extends IngestionException {

    private final String rawMessage;

    public BinanceParseException(String message, String rawMessage, Throwable cause) {
        super(null, message, cause);
        this.rawMessage = rawMessage;
    }

    public String rawMessage() {
        return rawMessage;
    }
}
