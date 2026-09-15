-- V006: Seed feature definitions for the microstructure computers wired into the
-- feature engine's dispatch loop alongside VWAP (see FeatureEngineRunner and
-- docs/steering/DETERMINISTIC_REPLAY.md). These rows are catalog/discovery metadata
-- for GET /api/v1/features (FeatureCatalogService) — the engine's own dispatch list
-- is code-defined, exactly as vwap.1m already was before this migration; see V004.
--
-- Source topics:
--   obi, micro_price -> events.book.snapshot (order-book state)
--   vpin             -> events.trade (executed trade flow, bucketed by volume)
--
-- window_duration is the engine's tumbling-window cadence for emission. For VPIN this
-- is the cadence at which a reading is *published*, not the bucket boundary itself —
-- VPIN's volume buckets do not align to time windows; see VPINComputer's Javadoc.

INSERT INTO feature_definitions (name, version, source_topic, window_duration, aggregation, instrument)
VALUES
    ('obi', 'v1', 'events.book.snapshot', INTERVAL '1 minute', 'OBI', 'BTC-USDT'),
    ('micro_price', 'v1', 'events.book.snapshot', INTERVAL '1 minute', 'MICRO_PRICE', 'BTC-USDT'),
    ('vpin', 'v1', 'events.trade', INTERVAL '1 minute', 'VPIN', 'BTC-USDT');
