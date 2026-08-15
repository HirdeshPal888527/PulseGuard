CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS window_metrics (
    window_start   TIMESTAMPTZ NOT NULL,
    window_end     TIMESTAMPTZ NOT NULL,
    user_id        TEXT NOT NULL,
    event_count    INTEGER NOT NULL,
    mean_amount    DOUBLE PRECISION NOT NULL,
    stddev_amount  DOUBLE PRECISION,
    max_amount     DOUBLE PRECISION,
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('window_metrics', 'window_start', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_window_metrics_user ON window_metrics (user_id, window_start DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    event_time     TIMESTAMPTZ NOT NULL,
    user_id        TEXT NOT NULL,
    amount         DOUBLE PRECISION NOT NULL,
    z_score        DOUBLE PRECISION NOT NULL,
    window_mean    DOUBLE PRECISION NOT NULL,
    window_stddev  DOUBLE PRECISION NOT NULL,
    location       TEXT,
    device_id      TEXT,
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('anomalies', 'event_time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_anomalies_user ON anomalies (user_id, event_time DESC);

SELECT add_retention_policy('window_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('anomalies', INTERVAL '7 days', if_not_exists => TRUE);
