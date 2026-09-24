-- Phase 5: priority ordering and per-job-type concurrency caps.

-- Higher number = more urgent. The opposite of Unix nice, chosen because an API
-- where priority: 10 beats priority: 1 needs no explanation to a caller.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;
ALTER TABLE recurring_jobs ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;

-- Supports the claim ordering. It cannot fully serve it once priority aging is
-- switched on, because the sort key is then an expression over now() and no
-- index can be precomputed for a moving target. It still narrows the scan to
-- eligible rows, which is the expensive part.
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, priority DESC, next_run_at);

-- A concurrency cap, not a token-bucket rate limit: "at most N of this type
-- running at once" rather than "N per second". For a job queue the binding
-- constraint is almost always how many simultaneous connections a downstream
-- service tolerates, which is a concurrency question. A per-second budget would
-- still allow N long-running jobs to pile up.
CREATE TABLE IF NOT EXISTS job_type_limits (
    type TEXT PRIMARY KEY,
    max_concurrency INT NOT NULL CHECK (max_concurrency > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Counting running jobs per type is the hot query for cap enforcement.
CREATE INDEX IF NOT EXISTS idx_jobs_running_by_type ON jobs (type) WHERE status = 'running';
