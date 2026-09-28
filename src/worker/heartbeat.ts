import { pool } from "../db/client";
import { config } from "../config";

/**
 * Keeps a lease alive while a job is being worked on, and detects when it has
 * been lost.
 *
 * The ownership predicate is the important part. If this worker stalls long
 * enough for its lease to lapse — a long GC pause, a frozen VM, a network
 * partition that heals — the reaper will have handed the job to someone else.
 * The stalled worker eventually wakes up believing it still owns the job. The
 * `worker_id = $2` clause means its heartbeat updates zero rows, which is how
 * it finds out it has been fenced out.
 *
 * This is the classic distributed-lock hazard, and it is worth being clear that
 * leases do not eliminate it: no timeout can distinguish "slow" from "dead".
 * The lease bounds how long a dead worker blocks a job; idempotency (phase 6)
 * is what makes the overlap harmless when the guess is wrong. Neither mechanism
 * is sufficient alone.
 */
export function startHeartbeat(
  jobId: number,
  workerId: string,
  onLost: () => void,
): () => void {
  let lost = false;

  const timer = setInterval(async () => {
    try {
      const { rowCount } = await pool.query(
        `UPDATE jobs
         SET lease_expires_at = now() + make_interval(secs => $3::double precision),
             updated_at = now()
         WHERE id = $1 AND worker_id = $2 AND status = 'running'`,
        [jobId, workerId, config.worker.leaseSeconds],
      );

      if (rowCount === 0 && !lost) {
        lost = true;
        console.error(
          `[worker ${workerId}] lost lease on job ${jobId} — it was reclaimed while this worker was stalled. Idempotency is the only thing preventing a duplicate effect here.`,
        );
        onLost();
      }
    } catch (err) {
      // A failed heartbeat is not fatal on its own: the lease still has slack
      // for another beat or two before it lapses.
      console.error(`[worker ${workerId}] heartbeat failed for job ${jobId}`, err);
    }
  }, config.worker.heartbeatIntervalMs);

  // unref so a pending heartbeat never holds the process open during shutdown.
  timer.unref();

  return () => clearInterval(timer);
}
