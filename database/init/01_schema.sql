-- Telemetry samples schema. This file is the contract between the collector
-- (writer) and the API gateway (reader). Loaded by the database component on init.

CREATE TABLE IF NOT EXISTS gpu_samples (
    id          BIGINT GENERATED ALWAYS AS IDENTITY,
    ts          TIMESTAMPTZ  NOT NULL,
    metric      TEXT         NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    uuid        TEXT         NOT NULL,
    gpu_index   TEXT         NOT NULL,
    device      TEXT         NOT NULL DEFAULT '',
    model_name  TEXT         NOT NULL DEFAULT '',
    hostname    TEXT         NOT NULL DEFAULT '',
    PRIMARY KEY (id)
);

-- Primary access pattern: telemetry for one GPU ordered by time.
CREATE INDEX IF NOT EXISTS idx_gpu_samples_uuid_ts
    ON gpu_samples (uuid, ts);

-- Support the "list all GPUs" query and metric filtering.
CREATE INDEX IF NOT EXISTS idx_gpu_samples_uuid
    ON gpu_samples (uuid);
CREATE INDEX IF NOT EXISTS idx_gpu_samples_metric_ts
    ON gpu_samples (uuid, metric, ts);

-- Idempotency for at-least-once delivery: a given (uuid, metric, ts) reading is
-- unique, so redelivered messages are ignored via ON CONFLICT DO NOTHING.
CREATE UNIQUE INDEX IF NOT EXISTS uq_gpu_samples_dedup
    ON gpu_samples (uuid, metric, ts);
