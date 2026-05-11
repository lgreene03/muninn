CREATE TABLE event_metadata (
    event_id       UUID PRIMARY KEY,
    event_type     VARCHAR(255) NOT NULL,
    event_time     TIMESTAMPTZ  NOT NULL,
    source         VARCHAR(255) NOT NULL,
    partition_key  VARCHAR(255),
    topic          VARCHAR(255) NOT NULL,
    kafka_partition INT          NOT NULL,
    kafka_offset   BIGINT       NOT NULL,
    ingested_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    parquet_path   VARCHAR(1024)
);

CREATE INDEX idx_event_metadata_type_time ON event_metadata (event_type, event_time);
CREATE INDEX idx_event_metadata_source ON event_metadata (source);

CREATE TABLE replay_cursors (
    consumer_group VARCHAR(255) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition_id   INT          NOT NULL,
    current_offset BIGINT       NOT NULL,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition_id)
);


