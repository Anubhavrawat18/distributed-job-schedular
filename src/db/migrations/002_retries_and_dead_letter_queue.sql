-- Phase 3: retries, backoff, and a dead-letter queue.
--
-- attempts / max_attempts make retry budget explicit per job rather than a
-- global constant, so a cheap idempotent job can afford 10 tries while a
-- payment call gets 2.
--
-- next_run_at is the backoff mechanism: a failed job goes back to 'pending'
-- with next_run_at in the future, and the claim query simply refuses to see it
-- until then. Backoff therefore costs nothing at runtime -- there is no timer,
-- no sleeping worker, no in-memory delay queue to lose on restart. The delay is
-- a row value, so it survives a crash like everything else.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 3;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- The claim filter is now (status, next_run_at), not (status, created_at).
CREATE INDEX IF NOT EXISTS idx_jobs_status_next_run_at ON jobs (status, next_run_at);
DROP INDEX IF EXISTS idx_jobs_status_created_at;

-- The dead-letter queue references jobs rather than moving rows out of it.
-- Moving the row would orphan its job_executions history -- exactly the
-- forensic trail you want when investigating why something died. Keeping the
-- job in place means "what happened to job 7" stays answerable, and a replay is
-- an UPDATE rather than a migration back across tables.
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL UNIQUE REFERENCES jobs (id) ON DELETE CASCADE,
    attempts INT NOT NULL,
    final_error TEXT NOT NULL,
    died_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_jobs_died_at ON dead_letter_jobs (died_at DESC);
