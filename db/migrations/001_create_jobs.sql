CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    payload BYTEA,
    status TEXT NOT NULL DEFAULT 'PENDING',
    priority INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    attempts INT NOT NULL DEFAULT 0,
    depends_on TEXT[] DEFAULT '{}',
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

CREATE INDEX idx_jobs_status_scheduled 
ON jobs(status, scheduled_at);

CREATE INDEX idx_jobs_priority 
ON jobs(priority DESC);