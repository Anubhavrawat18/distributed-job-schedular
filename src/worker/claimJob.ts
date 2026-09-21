import { pool } from "../db/client";
import { config } from "../config";
import type { ClaimStrategy } from "../config";
import type { Job } from "../types/job";

// overall working of the function
// Database -> Find oldest pending job -> No job? -> return null -> if found job -> change status from pendin to running ->return that job

/**
 * Concurrency-safe claim: one statement, so the read and the write cannot be
 * separated by another transaction.
 *
 * FOR UPDATE locks the row the subquery picks. SKIP LOCKED is the half that
 * matters for throughput: a worker whose candidate row is already locked skips
 * to the next unlocked row instead of blocking behind the lock. Without it, N
 * workers would queue up on the same row and effectively run single-file; with
 * it, N workers claim N different jobs in parallel.
 *
 * LIMIT 1 sits inside the subquery, so each worker locks exactly one row.
 *
 * `next_run_at <= now()` is what implements retry backoff: a job waiting out
 * its delay is simply invisible to this query until its time arrives. The
 * attempt counter is incremented here rather than in executeJob so that it is
 * atomic with the claim — a worker that crashes mid-job has still spent an
 * attempt, which is what stops a poison job from being retried forever.
 */
async function claimSkipLocked(workerId: string): Promise<Job | null> {
  const { rows } = await pool.query<Job>(
    `UPDATE jobs
     SET status = 'running', worker_id = $1, attempts = attempts + 1, updated_at = now()
     WHERE id = (
       SELECT id FROM jobs
       WHERE status = 'pending' AND next_run_at <= now()
       ORDER BY next_run_at, created_at
       FOR UPDATE SKIP LOCKED
       LIMIT 1
     )
     RETURNING *`,
    [workerId],
  );

  return rows[0] ?? null;
}

/**
 * The Phase 1 claim, kept so the bug is reproducible rather than just described.
 *
 * The SELECT and the UPDATE are separate statements with no lock held between
 * them, so two workers can both read the same row as 'pending' and both go on to
 * execute it. Run the fleet with CLAIM_STRATEGY=naive and the duplicate-execution
 * check in tests/concurrency.ts fails; switch back to skip_locked and it passes.
 */
async function claimNaive(workerId: string): Promise<Job | null> {
  // select exactly one job with pending status
  const pending = await pool.query<{ id: number }>(
    `SELECT id FROM jobs
     WHERE status = 'pending' AND next_run_at <= now()
     ORDER BY next_run_at, created_at
     LIMIT 1`,
  );

  if (pending.rows.length === 0) {
    return null;
  }

  // The race lives in the gap between the statement above and the one below.
  // No artificial delay is needed to hit it: with 5 workers, ~48% of jobs get
  // claimed more than once.
  const claimed = await pool.query<Job>(
    `UPDATE jobs
     SET status = 'running', worker_id = $1, attempts = attempts + 1, updated_at = now()
     WHERE id = $2
     RETURNING *`,
    [workerId, pending.rows[0].id],
  );

  return claimed.rows[0] ?? null;
}

export async function claimJob(
  workerId: string,
  strategy: ClaimStrategy = config.worker.claimStrategy,
): Promise<Job | null> {
  return strategy === "naive"
    ? claimNaive(workerId)
    : claimSkipLocked(workerId);
}
