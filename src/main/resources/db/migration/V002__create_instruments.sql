-- V002: Create instruments reference table
-- Stores instrument metadata. Read by ingestion-service and query-api.
-- See DOMAIN_MODEL.md and DATA_STORAGE_STRATEGY.md.

CREATE TABLE instruments (
    symbol      VARCHAR(50) PRIMARY KEY,
    base_asset  VARCHAR(20) NOT NULL,
    quote_asset VARCHAR(20) NOT NULL,
    exchange_id VARCHAR(50) NOT NULL REFERENCES exchanges(id),
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_instruments_exchange ON instruments(exchange_id);
