-- V003: Seed reference data for MVP
-- Binance exchange and BTC-USDT instrument for Phase 1.

INSERT INTO exchanges (id, display_name, timezone)
VALUES ('binance', 'Binance Spot', 'UTC');

INSERT INTO instruments (symbol, base_asset, quote_asset, exchange_id)
VALUES ('BTC-USDT', 'BTC', 'USDT', 'binance');
