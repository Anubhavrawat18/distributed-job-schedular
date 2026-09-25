-- Phase 7: indexes for the dashboard's read path.
--
-- The dashboard polls, so its queries run far more often than any single job is
-- claimed. Without these they are sequential scans over tables that only grow,
-- and the dashboard would slowly turn into the thing that starves the workers it
-- is meant to be watching.

-- Throughput ("executions in the last minute") is a time-window query over the
-- largest table in the schema.
CREATE INDEX IF NOT EXISTS idx_job_executions_started_at
    ON job_executions (started_at DESC);

-- "Which worker did what recently" — the fleet view.
CREATE INDEX IF NOT EXISTS idx_job_executions_worker_started
    ON job_executions (worker_id, started_at DESC);

-- Recently finished work, for throughput and the completed/failed timeline.
CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs (updated_at DESC);

-- Per-type queue depth, the breakdown the dashboard groups by.
CREATE INDEX IF NOT EXISTS idx_jobs_type_status ON jobs (type, status);
