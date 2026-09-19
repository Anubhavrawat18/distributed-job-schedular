import { pool } from "../db/client";
import type { Job } from "../types/job";

/**
 * Phase 1 claim: read the oldest pending job, then flip it to 'running'.
 *
 * This is deliberately the naive version. Two workers running this at the same
 * time can both read the same row and both execute it — the read and the write
 * are separate statements with no lock between them. Phase 2 replaces it with
 * `FOR UPDATE SKIP LOCKED`; keeping the naive version first makes that fix
 * (and the race it closes) demonstrable.
 */

// overall working of the function
// Database -> Find oldest pending job -> No job? -> return null -> if found job -> change status from pendin to running ->return that job

export async function claimJob(): Promise<Job | null> {
  // select exactly one job with pending status
  const pending = await pool.query<Job>(
    `SELECT id FROM jobs
         WHERE status = 'pending'
         ORDER BY created_at
         LIMIT 1`,
  );

  if (pending.rows.length === 0) {
    return null;
  }

  const claimed = await pool.query<Job>(
    `UPDATE jobs
         SET status = 'running', updated_at = now()
         WHERE id = $1
         RETURNING *`,
    [pending.rows[0].id],
  );

  return claimed.rows[0];
}
