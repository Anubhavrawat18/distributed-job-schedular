-- Phase 1: basic job queue.
--
-- status lifecycle: pending -> running -> (completed | failed)
-- Later phases add columns for retries, scheduling, priority, idempotency, etc.
-- Kept minimal here on purpose so each phase's diff is easy to review.

CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Workers poll for the oldest pending job, so an index on (status, created_at)
-- keeps that lookup cheap as the table grows.
CREATE INDEX IF NOT EXISTS idx_jobs_status_created_at ON jobs (status, created_at);
