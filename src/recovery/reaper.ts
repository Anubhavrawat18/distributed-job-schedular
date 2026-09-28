import { pool } from "../db/client";
import { config } from "../config";
import { computeBackoffMs } from "../retry/backoff";

/**
 * Returns jobs abandoned by dead workers to the queue.
 *
 * A job is abandoned when its lease has lapsed: the worker holding it stopped
 * heartbeating. No cooperation from that worker is required or expected — the
 * whole reason leases exist is that a SIGKILLed, OOM-killed or partitioned
 * process runs no cleanup code at all.
 *
 * A reclaimed job goes back to 'pending' with the same backoff a failure would
 * get, rather than being retried instantly. A job that killed its worker is
 * likelier than average to kill the next one, and hammering it immediately is
 * how a single poison job takes down a whole fleet in sequence.
 *
 * `attempts` is deliberately not incremented here: it was already charged at
 * claim time (phase 3), precisely so a crashed execution still costs an
 * attempt. That is what bounds this loop — a job that reliably kills workers
 * exhausts its budget and dead-letters instead of cycling forever.
 *
 * The scan and the reclaims share one transaction so `FOR UPDATE SKIP LOCKED`
 * actually holds for the duration. Every worker runs this, so several reapers
 * can scan at once; SKIP LOCKED lets them divide the work instead of colliding.
 */
export async function reapExpiredLeases(reaperId: string): Promise<number> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const { rows: expired } = await client.query<{
      id: number;
      type: string;
      worker_id: string | null;
      attempts: number;
      max_attempts: number;
    }>(
      `SELECT id, type, worker_id, attempts, max_attempts
       FROM jobs
       WHERE status = 'running' AND lease_expires_at < now()
       ORDER BY lease_expires_at
       FOR UPDATE SKIP LOCKED
       LIMIT 20`,
    );

    if (expired.length === 0) {
      await client.query("COMMIT");
      return 0;
    }

    let reclaimed = 0;

    for (const job of expired) {
      // Out of attempts: dead-letter rather than return it to the queue, so a
      // job that kills every worker it touches stops being handed out.
      if (job.attempts >= job.max_attempts) {
        const error = `worker ${job.worker_id ?? "unknown"} died holding this job; retry budget exhausted after ${job.attempts} attempt(s)`;

        await client.query(
          `UPDATE jobs
           SET status = 'failed', error = $2, lease_expires_at = NULL,
               reclaim_count = reclaim_count + 1, updated_at = now()
           WHERE id = $1`,
          [job.id, error],
        );

        await client.query(
          `INSERT INTO dead_letter_jobs (job_id, attempts, final_error)
           VALUES ($1, $2, $3)
           ON CONFLICT (job_id) DO NOTHING`,
          [job.id, job.attempts, error],
        );

        console.error(`[reaper ${reaperId}] job ${job.id} dead-lettered: ${error}`);
        reclaimed++;
        continue;
      }

      const delayMs = computeBackoffMs(job.attempts, {
        baseMs: config.retry.baseMs,
        maxMs: config.retry.maxMs,
      });

      await client.query(
        `UPDATE jobs
         SET status = 'pending',
             worker_id = NULL,
             lease_expires_at = NULL,
             reclaim_count = reclaim_count + 1,
             error = $2,
             next_run_at = now() + make_interval(secs => $3::double precision),
             updated_at = now()
         WHERE id = $1`,
        [
          job.id,
          `reclaimed after worker ${job.worker_id ?? "unknown"} stopped heartbeating`,
          delayMs / 1000,
        ],
      );

      reclaimed++;
      console.warn(
        `[reaper ${reaperId}] reclaimed job ${job.id} (${job.type}) from ${job.worker_id ?? "unknown"}, retrying in ${delayMs}ms (attempt ${job.attempts}/${job.max_attempts})`,
      );
    }

    await client.query("COMMIT");
    return reclaimed;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
