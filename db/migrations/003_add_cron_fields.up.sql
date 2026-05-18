ALTER TABLE jobs
ADD COLUMN cron_expression TEXT,
ADD COLUMN next_run_at TIMESTAMPTZ;

CREATE INDEX idx_jobs_next_run
ON jobs(next_run_at)
WHERE cron_expression IS NOT NULL;