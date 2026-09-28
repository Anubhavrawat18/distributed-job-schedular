import type { PoolClient } from "pg";
import { pool } from "../db/client";
import { config } from "../config";
import { computeBackoffMs } from "../retry/backoff";
import { withIdempotency, type IdempotentOutcome } from "../idempotency/withIdempotency";
import type { Job } from "../types/job";

interface JobContext {
  attempt: number;
  maxAttempts: number;
  jobId: number;

  /**
   * Performs `fn` at most once across every attempt of this job. The key is
   * scoped to the job, so retries of the same job share it while unrelated jobs
   * never collide.
   */
  idempotent: <T>(
    name: string,
    fn: (client: PoolClient) => Promise<T>,
  ) => Promise<IdempotentOutcome<T>>;
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

  // Trivial no-op, used as the body of recurring jobs in tests.
  tick: async (payload) => ({ tickedAt: new Date().toISOString(), ...payload }),

  /**
   * Performs a side effect and then dies, which is the exact shape of the bug
   * idempotency exists to fix: the effect committed, but the worker never got
   * to record that the job finished, so the attempt is retried.
   *
   * With IDEMPOTENCY=enforced the effect is written once no matter how many
   * attempts run. With IDEMPOTENCY=off the row count equals the attempt count.
   */
  "charge-then-crash": async (payload, ctx) => {
    const label = String(payload.label ?? `job-${ctx.jobId}`);
    const crashUntilAttempt = Number(payload.crashUntilAttempt ?? 1);

    const outcome = await ctx.idempotent("charge", async (client) => {
      const { rows } = await client.query<{ id: number }>(
        `INSERT INTO side_effects (job_id, label) VALUES ($1, $2) RETURNING id`,
        [ctx.jobId, label],
      );
      return { sideEffectId: rows[0].id };
    });

    // The "crash": the effect is committed, this attempt then fails. A real
    // crash would be a killed process; throwing here is the deterministic
    // equivalent and exercises the same retry path.
    if (ctx.attempt <= crashUntilAttempt) {
      throw new Error(`crashed after performing side effect on attempt ${ctx.attempt}`);
    }

    return { ...outcome.result, repeatedEffect: !outcome.executed };
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
    await deadLetter(
      job,
      `no handler registered for job type "${job.type}"`,
      workerId,
    );
    return;
  }

  try {
    const result = await handler(job.payload, {
      attempt: job.attempts,
      maxAttempts: job.max_attempts,
      jobId: job.id,
      idempotent: (name, fn) =>
        withIdempotency(`job:${job.id}:${name}`, job.id, fn),
    });
    // `worker_id = $3` fences a worker that stalled long enough to lose its
    // lease: if the reaper handed this job to someone else, this update matches
    // no rows instead of overwriting the new owner's outcome.
    const { rowCount } = await pool.query(
      `UPDATE jobs
             SET status = 'completed', result = $2, error = NULL,
                 lease_expires_at = NULL, updated_at = now()
             WHERE id = $1 AND worker_id = $3`,
      [job.id, result, workerId],
    );

    if (rowCount === 0) {
      console.warn(
        `[worker ${workerId}] finished job ${job.id} but no longer owns it; discarding the result`,
      );
      return;
    }
    console.log(
      `[worker] job ${job.id} (${job.type}) completed on attempt ${job.attempts}`,
    );
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await handleFailure(job, message, workerId);
  }
}

/**
 * Retry budget check. `attempts` was already incremented at claim time, so it
 * reflects the attempt that just failed.
 */
async function handleFailure(
  job: Job,
  error: string,
  workerId: string,
): Promise<void> {
  if (job.attempts >= job.max_attempts) {
    await deadLetter(job, error, workerId);
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
         worker_id = NULL,
         lease_expires_at = NULL,
         error = $2,
         next_run_at = now() + make_interval(secs => $3::double precision),
         updated_at = now()
     WHERE id = $1 AND worker_id = $4`,
    [job.id, error, delayMs / 1000, workerId],
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
async function deadLetter(
  job: Job,
  error: string,
  workerId: string,
): Promise<void> {
  await pool.query(
    `UPDATE jobs
     SET status = 'failed', error = $2, lease_expires_at = NULL, updated_at = now()
     WHERE id = $1 AND worker_id = $3`,
    [job.id, error, workerId],
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
