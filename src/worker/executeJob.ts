import { pool } from "../db/client";
import { config } from "../config";
import { computeBackoffMs } from "../retry/backoff";
import type { Job } from "../types/job";

interface JobContext {
  attempt: number;
  maxAttempts: number;
}

type JobHandler = (
  payload: Record<string, unknown>,
  ctx: JobContext,
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

  // Fails its first `failTimes` attempts, then succeeds — the shape of a real
  // transient fault, and what proves retries actually recover a job rather than
  // just delaying its death.
  flaky: async (payload, ctx) => {
    const failTimes = Number(payload.failTimes ?? 1);
    if (ctx.attempt <= failTimes) {
      throw new Error(`transient failure on attempt ${ctx.attempt}`);
    }
    return { succeededOnAttempt: ctx.attempt };
  },
};

// Find the handler for the job type
// → If no handler exists, mark the job as failed
// → Otherwise, execute the handler
// → If execution succeeds, mark the job as completed and store the result
// → If execution fails, catch the error and either schedule a retry or dead-letter it

export async function executeJob(job: Job, workerId: string): Promise<void> {
  // Append-only record of "this worker actually ran this job". Written before
  // the handler, so a duplicate claim is recorded even if both executions end
  // up overwriting each other's final status on the jobs row.
  await pool.query(
    `INSERT INTO job_executions (job_id, worker_id) VALUES ($1, $2)`,
    [job.id, workerId],
  );

  const handler = handlers[job.type];

  // An unknown job type is not a transient fault — retrying cannot make a
  // handler appear — so it skips the backoff path and dead-letters immediately.
  if (!handler) {
    await deadLetter(job, `no handler registered for job type "${job.type}"`);
    return;
  }

  try {
    const result = await handler(job.payload, {
      attempt: job.attempts,
      maxAttempts: job.max_attempts,
    });
    await pool.query(
      `UPDATE jobs
             SET status = 'completed', result = $2, error = NULL, updated_at = now()
             WHERE id = $1`,
      [job.id, result],
    );
    console.log(
      `[worker] job ${job.id} (${job.type}) completed on attempt ${job.attempts}`,
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await handleFailure(job, message);
  }
}

/**
 * Retry budget check. `attempts` was already incremented at claim time, so it
 * reflects the attempt that just failed.
 */
async function handleFailure(job: Job, error: string): Promise<void> {
  if (job.attempts >= job.max_attempts) {
    await deadLetter(job, error);
    return;
  }

  const delayMs = computeBackoffMs(job.attempts, {
    baseMs: config.retry.baseMs,
    maxMs: config.retry.maxMs,
  });

  // Back to 'pending', but invisible to the claim query until next_run_at
  // passes. The delay lives in the row, so it survives a worker restart —
  // nothing is holding a timer in memory.
  await pool.query(
    `UPDATE jobs
     SET status = 'pending',
         error = $2,
         next_run_at = now() + make_interval(secs => $3::double precision),
         updated_at = now()
     WHERE id = $1`,
    [job.id, error, delayMs / 1000],
  );

  console.warn(
    `[worker] job ${job.id} failed on attempt ${job.attempts}/${job.max_attempts}, retrying in ${delayMs}ms: ${error}`,
  );
}

/**
 * Terminal failure. The job stays in `jobs` with status 'failed' and gains a
 * dead_letter_jobs row recording why it died — the job is not moved, so its
 * job_executions history stays intact for investigation.
 */
async function deadLetter(job: Job, error: string): Promise<void> {
  await pool.query(
    `UPDATE jobs
     SET status = 'failed', error = $2, updated_at = now()
     WHERE id = $1`,
    [job.id, error],
  );

  // ON CONFLICT: a job can only die once, but a duplicate claim under the naive
  // strategy could try to dead-letter it twice.
  await pool.query(
    `INSERT INTO dead_letter_jobs (job_id, attempts, final_error)
     VALUES ($1, $2, $3)
     ON CONFLICT (job_id) DO NOTHING`,
    [job.id, job.attempts, error],
  );

  console.error(
    `[worker] job ${job.id} dead-lettered after ${job.attempts} attempt(s): ${error}`,
  );
}
