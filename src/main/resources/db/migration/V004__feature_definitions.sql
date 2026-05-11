-- V004: Feature definitions and checkpoint tracking
-- Supports the feature engine's registration, versioning, and checkpoint restore.

CREATE TABLE feature_definitions (
    name            VARCHAR(100) PRIMARY KEY,
    version         VARCHAR(50)  NOT NULL,
    source_topic    VARCHAR(100) NOT NULL,
    window_duration INTERVAL     NOT NULL,
    aggregation     VARCHAR(50)  NOT NULL,
    instrument      VARCHAR(50)  NOT NULL REFERENCES instruments(symbol),
    active          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE feature_checkpoints (
    id              BIGSERIAL     PRIMARY KEY,
    feature_name    VARCHAR(100)  NOT NULL REFERENCES feature_definitions(name),
    feature_version VARCHAR(50)   NOT NULL,
    watermark       TIMESTAMPTZ   NOT NULL,
    checkpoint_path VARCHAR(500)  NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_feature_checkpoints_lookup
    ON feature_checkpoints (feature_name, feature_version, watermark DESC);

-- Seed: 1-minute VWAP for BTC-USDT
INSERT INTO feature_definitions (name, version, source_topic, window_duration, aggregation, instrument)
VALUES ('vwap.1m', 'v1', 'events.trade', INTERVAL '1 minute', 'VWAP', 'BTC-USDT');
