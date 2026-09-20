import { pool } from "../db/client";
import type { Job } from "../types/job";

type JobHandler = (
  payload: Record<string, unknown>,
) => Promise<Record<string, unknown>>;

const handlers: Record<string, JobHandler> = {
  // Stand-in for real work: sleeps for `ms` so queue behaviour is observable.
  sleep: async (payload) => {
    const ms = Number(payload.ms ?? 100);
    await new Promise((resolve) => setTimeout(resolve, ms));
    return { sleptMs: ms };
  },

  // Deterministic failure, used to exercise the failed-job path.
  fail: async (payload) => {
    throw new Error(String(payload.message ?? "job asked to fail"));
  },
};

// Find the handler for the job type
// → If no handler exists, mark the job as failed
// → Otherwise, execute the handler
// → If execution succeeds, mark the job as completed and store the result
// → If execution fails, catch the error and mark the job as failed

export async function executeJob(job: Job, workerId: string): Promise<void> {
  // Append-only record of "this worker actually ran this job". Written before
  // the handler, so a duplicate claim is recorded even if both executions end
  // up overwriting each other's final status on the jobs row.
  await pool.query(
    `INSERT INTO job_executions (job_id, worker_id) VALUES ($1, $2)`,
    [job.id, workerId],
  );

  const handler = handlers[job.type];

  if (!handler) {
    await markFailed(
      job.id,
      `no handler registered for job type "${job.type}"`,
    );
    return;
  }

  try {
    const result = await handler(job.payload);
    await pool.query(
      `UPDATE jobs
             SET status = 'completed', result = $2, error = NULL, updated_at = now()
             WHERE id = $1`,
      [job.id, result],
    );
    console.log(`[worker] job ${job.id} (${job.type}) completed`);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await markFailed(job.id, message);
  }
}

// Phase 1 is terminal on failure — no retries yet. Phase 3 adds backoff + DLQ.
async function markFailed(id: number, error: string): Promise<void> {
  await pool.query(
    `UPDATE jobs
         SET status = 'failed', error = $2, updated_at = now()
         WHERE id = $1`,
    [id, error],
  );
  console.error(`[worker] job ${id} failed: ${error}`);
}
