-- V001: Create exchanges reference table
-- Stores exchange metadata. Read by ingestion-service and query-api.
-- See DOMAIN_MODEL.md and DATA_STORAGE_STRATEGY.md.

CREATE TABLE exchanges (
    id          VARCHAR(50) PRIMARY KEY,
    display_name VARCHAR(100) NOT NULL,
    timezone    VARCHAR(50)  NOT NULL DEFAULT 'UTC',
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
