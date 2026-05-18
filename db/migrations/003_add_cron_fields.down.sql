DROP INDEX IF EXISTS idx_jobs_next_run;
ALTER TABLE jobs 
DROP COLUMN IF EXISTS cron_expression,
DROP COLUMN IF EXISTS next_run_at;