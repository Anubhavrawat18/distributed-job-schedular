-- Phase 2: concurrency-safe claiming.
--
-- Two additions:
--   1. jobs.worker_id  — which worker currently holds a job. Makes a claim
--      attributable instead of anonymous, and is what Phase 8 will use to
--      detect a job whose worker died mid-execution.
--   2. job_executions  — one row per execution attempt.
--
-- job_executions is the evidence that SKIP LOCKED works. Asserting on
-- jobs.status alone cannot detect a double-run: if two workers execute the same
-- job, the second UPDATE simply overwrites the first and the row still reads
-- 'completed' exactly once. A separate append-only table cannot be overwritten,
-- so `HAVING count(*) > 1` is a direct, honest measurement of duplicate work.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS worker_id TEXT;

CREATE TABLE IF NOT EXISTS job_executions (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_executions_job_id ON job_executions (job_id);
