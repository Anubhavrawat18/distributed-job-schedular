-- Phase 4: delayed and recurring jobs.
--
-- Delayed jobs need NO new column. next_run_at (added in phase 3 for retry
-- backoff) already means "not eligible until this instant", and the claim query
-- already filters on it. A delayed job is just a job whose first next_run_at is
-- in the future. Adding a separate run_at column would create a second source
-- of truth for the same question and force the claim to consult both.

-- A recurring_job is a *definition*, not a job. The scheduler materialises it
-- into concrete rows in `jobs` as each occurrence comes due, which keeps
-- execution history per-occurrence rather than collapsing it into the schedule.
CREATE TABLE IF NOT EXISTS recurring_jobs (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    cron_expression TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    max_attempts INT NOT NULL DEFAULT 3,
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- When this definition next produces a job. Advanced atomically as the
    -- scheduler claims it, which is what stops two scheduler replicas from
    -- materialising the same occurrence.
    next_run_at TIMESTAMPTZ NOT NULL,
    last_enqueued_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_recurring_jobs_due ON recurring_jobs (enabled, next_run_at);

-- Provenance: which definition produced this job, and which occurrence it is.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS recurring_job_id BIGINT
    REFERENCES recurring_jobs (id) ON DELETE SET NULL;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ;

-- The hard guarantee that one occurrence produces one job, enforced by the
-- database rather than by the scheduler being careful. Claiming a definition
-- under SKIP LOCKED should already make a duplicate impossible; this makes it
-- impossible even if that logic is wrong, a scheduler is rolled back to an old
-- build, or someone inserts by hand. Correctness that depends only on
-- application code holding is correctness you cannot prove.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_recurring_occurrence
    ON jobs (recurring_job_id, scheduled_for)
    WHERE recurring_job_id IS NOT NULL;

