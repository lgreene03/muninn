/**
 * Ingestion service for normalized market data from external exchanges.
 *
 * <p>Handles connecting to exchange APIs (e.g., Binance), parsing
 * native JSON payloads into canonical {@code MarketEvent}s,
 * and producing them to the Redpanda event log.</p>
 */
package io.muninn.ingestion;
