-- Phase 8: leases and crash recovery.
--
-- The gap this closes has been open since phase 2. A worker that dies while
-- holding a job leaves that row in 'running' forever: nothing re-claims it,
-- because the claim query only looks at 'pending'. Every guarantee built since
-- then -- retries, backoff, the dead-letter queue, idempotent re-execution --
-- is unreachable in the one case they were designed for, because the job never
-- becomes claimable again.
--
-- A lease is a claim with an expiry. The worker must keep saying "still working
-- on it"; if it stops saying so, the job is presumed abandoned and returned to
-- the queue. Crucially this needs no cooperation from the dead worker, which is
-- the point -- a SIGKILLed process, an OOM, or a severed network link all run
-- exactly zero lines of cleanup code.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

-- How many times a job has been reclaimed from a dead worker. Distinct from
-- `attempts`, which counts executions: this counts abandonment specifically, so
-- a job that keeps killing its workers is identifiable as such rather than
-- looking like an ordinary flaky job.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS reclaim_count INT NOT NULL DEFAULT 0;

-- The reaper's only query: running jobs whose lease has lapsed. Partial, since
-- 'running' is a tiny slice of the table and the reaper polls frequently.
CREATE INDEX IF NOT EXISTS idx_jobs_expired_leases
    ON jobs (lease_expires_at)
    WHERE status = 'running';
