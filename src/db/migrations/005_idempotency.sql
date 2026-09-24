-- Phase 6: idempotency.
--
-- There are two separate duplication problems, and they need different fixes.
--
--   1. Enqueue duplication. A client POSTs a job, the response is lost to a
--      network timeout, the client retries. Two identical jobs now exist. The
--      client cannot tell the difference between "request lost" and "response
--      lost", so it must be safe for it to retry.
--
--   2. Execution duplication. A worker performs a side effect, then dies before
--      recording completion. Its attempt is retried and the side effect happens
--      twice. Phase 2's SKIP LOCKED does not help here: both executions are
--      legitimately sequential, not concurrent.
--
-- Neither is solved by "executing exactly once", which is not achievable -- the
-- worker cannot atomically perform an effect and record that it did. What is
-- achievable is at-least-once execution with effects that are safe to repeat,
-- which observers cannot distinguish from exactly-once.

-- (1) Enqueue idempotency. A client-supplied key that collapses retries of the
-- same request onto the same job row.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- (2) Execution idempotency. A durable record that a named effect has already
-- been performed, plus the result it produced, so a repeat execution can return
-- the original answer instead of redoing the work.
--
-- The record is written in the SAME transaction as the effect it guards. That
-- is the whole trick: if they were separate, a crash between them would either
-- lose the effect or duplicate it, which is the problem we started with.
CREATE TABLE IF NOT EXISTS idempotency_records (
    key TEXT PRIMARY KEY,
    job_id BIGINT REFERENCES jobs (id) ON DELETE CASCADE,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_idempotency_records_job_id ON idempotency_records (job_id);

-- An observable stand-in for a real side effect (charging a card, sending an
-- email), in the same spirit as the `sleep` and `fail` handlers. Counting rows
-- here is how the tests tell "ran once" from "ran twice".
CREATE TABLE IF NOT EXISTS side_effects (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT,
    label TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_side_effects_label ON side_effects (label);
