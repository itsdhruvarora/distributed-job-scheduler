CREATE TABLE dead_letter_queue (
    id           TEXT PRIMARY KEY,
    job_id       TEXT NOT NULL REFERENCES jobs(id),
    error        TEXT NOT NULL,
    failed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload      BYTEA,
    job_type     TEXT NOT NULL
);

CREATE INDEX idx_dlq_failed_at ON dead_letter_queue(failed_at DESC);