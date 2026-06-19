-- Dieksekusi otomatis saat lifespan startup di db.py

CREATE TABLE IF NOT EXISTS processed_events (
    id              BIGSERIAL    PRIMARY KEY,
    topic           TEXT         NOT NULL,
    event_id        TEXT         NOT NULL,
    source          TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    event_timestamp TIMESTAMPTZ  NOT NULL,
    received_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_topic_event UNIQUE (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_pe_topic    ON processed_events(topic);
CREATE INDEX IF NOT EXISTS idx_pe_received ON processed_events(received_at DESC);

CREATE TABLE IF NOT EXISTS stats (
    key   TEXT   PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stats(key, value) VALUES
    ('received',          0),
    ('unique_processed',  0),
    ('duplicate_dropped', 0)
ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGSERIAL    PRIMARY KEY,
    event_time TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    action     TEXT         NOT NULL,  -- 'inserted' | 'duplicate' | 'error'
    topic      TEXT,
    event_id   TEXT,
    detail     JSONB
);
