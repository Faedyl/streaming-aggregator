-- PostgreSQL Initialization Script for UAS Pub-Sub Aggregator
-- Membuat tabel-tabel yang diperlukan

-- ============================================================
-- 1. Dedup Store: Mencegah event duplikat
--    UNIQUE constraint (topic, event_id) untuk idempotency
-- ============================================================
CREATE TABLE IF NOT EXISTS processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    event_source VARCHAR(255) NOT NULL DEFAULT '',
    event_payload JSONB NOT NULL DEFAULT '{}',
    event_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_events_topic
    ON processed_events(topic);
CREATE INDEX IF NOT EXISTS idx_processed_events_event_id
    ON processed_events(event_id);
CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events(processed_at DESC);

-- ============================================================
-- 2. Outbox Table: Untuk outbox pattern
--    Setiap item outbox diproses sekali (processed = false)
-- ============================================================
CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}',
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_outbox_status
    ON outbox(status)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_created_at
    ON outbox(created_at);

-- ============================================================
-- 3. Statistics Table: Menyimpan counter secara transaksional
--    UPDATE ... SET count = count + 1 untuk mencegah lost-update
-- ============================================================
CREATE TABLE IF NOT EXISTS event_stats (
    id SERIAL PRIMARY KEY,
    stat_key VARCHAR(100) NOT NULL UNIQUE,
    stat_value BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Insert initial stats rows
INSERT INTO event_stats (stat_key, stat_value)
VALUES
    ('received', 0),
    ('unique_processed', 0),
    ('duplicate_dropped', 0),
    ('outbox_processed', 0)
ON CONFLICT (stat_key) DO NOTHING;

-- ============================================================
-- 4. Audit Log: Untuk logging deteksi duplikasi
-- ============================================================
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    action VARCHAR(50) NOT NULL,
    details JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_created_at
    ON audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action
    ON audit_log(action);

-- ============================================================
-- Fungsi untuk increment stat atomically
-- ============================================================
CREATE OR REPLACE FUNCTION increment_stat(p_key VARCHAR(100), p_increment BIGINT DEFAULT 1)
RETURNS BIGINT AS $$
DECLARE
    new_value BIGINT;
BEGIN
    INSERT INTO event_stats (stat_key, stat_value)
    VALUES (p_key, p_increment)
    ON CONFLICT (stat_key)
    DO UPDATE SET stat_value = event_stats.stat_value + p_increment,
                  updated_at = NOW()
    RETURNING stat_value INTO new_value;
    RETURN new_value;
END;
$$ LANGUAGE plpgsql;
