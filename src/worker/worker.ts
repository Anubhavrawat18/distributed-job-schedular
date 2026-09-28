import { config } from "../config";
import { pool } from "../db/client";
import { claimJob } from "./claimJob";
import { executeJob } from "./executeJob";
import { startHeartbeat } from "./heartbeat";
import { reapExpiredLeases } from "../recovery/reaper";

// main loop of a background job worker
// Keep checking the database for jobs → claim one → execute it → immediately check for another → stop gracefully when told to

//   Start worker
//        ↓
//    claimJob()
//        ↓
//  Is there a job?
//  ┌─────┴─────┐
// No          Yes
//  ↓           ↓
// sleep     claim job
//  ↓           ↓
// retry    executeJob(job)
//              ↓
//      immediately claim next job

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let running = true;
let currentJobId: number | null = null;

const workerId = config.worker.id;

async function main(): Promise<void> {
  console.log(
    `[worker ${workerId}] started, polling every ${config.worker.pollIntervalMs}ms, claim strategy: ${config.worker.claimStrategy}, lease ${config.worker.leaseSeconds}s`,
  );

  while (running) {
    let job;
    try {
      job = await claimJob(workerId);
    } catch (err) {
      console.error("[worker] claim failed", err);
      await sleep(config.worker.pollIntervalMs);
      continue;
    }

    if (!job) {
      await sleep(config.worker.pollIntervalMs);
      continue;
    }

    console.log(`[worker ${workerId}] claimed job ${job.id} (${job.type})`);

    currentJobId = job.id;
    const stopHeartbeat = startHeartbeat(job.id, workerId, () => {
      // Fenced out. The job now belongs to someone else; executeJob's terminal
      // writes are ownership-guarded, so finishing is harmless — the result is
      // simply discarded.
    });

    try {
      await executeJob(job, workerId);
    } finally {
      stopHeartbeat();
      currentJobId = null;
    }
    // Drain back-to-back without sleeping while work remains in the queue.
  }

  await pool.end();
  console.log(`[worker ${workerId}] stopped`);
}

/**
 * Every worker reaps, on its own timer independent of the claim loop.
 *
 * Deliberately not a dedicated reaper process: recovery would then have a
 * single point of failure, and the one component whose death strands jobs
 * forever should not be the component whose job is to un-strand them.
 */
function startReaper(): void {
  const timer = setInterval(async () => {
    if (!running) return;
    try {
      await reapExpiredLeases(workerId);
    } catch (err) {
      console.error(`[reaper ${workerId}] scan failed`, err);
    }
  }, config.worker.reapIntervalMs);

  timer.unref();
}

/**
 * Graceful shutdown.
 *
 * This is an optimisation, not the safety mechanism. It makes the common case
 * — a deploy, a scale-down, anything that sends SIGTERM — fast and tidy: stop
 * claiming immediately, let the in-flight job finish, exit. Without it a
 * redeploy would strand each worker's current job until its lease lapsed.
 *
 * The lease is what makes shutdown *safe*, and it covers everything this cannot:
 * SIGKILL, an OOM kill, a severed network link, a yanked power cable. In those
 * cases none of this code runs at all. That is the division of labour — this
 * path is fast but requires cooperation, the lease is slower but requires none.
 *
 * If the in-flight job outlasts the grace period, the job is handed back
 * explicitly rather than left to expire, so a redeploy never parks a job for a
 * full lease period.
 */
async function shutdown(signal: string): Promise<void> {
  if (!running) return;
  running = false;

  console.log(
    `[worker ${workerId}] ${signal} received, no longer claiming; up to ${config.worker.shutdownGraceMs}ms for the in-flight job`,
  );

  const deadline = Date.now() + config.worker.shutdownGraceMs;
  while (currentJobId !== null && Date.now() < deadline) {
    await sleep(100);
  }

  if (currentJobId !== null) {
    const strandedId = currentJobId;
    console.warn(
      `[worker ${workerId}] job ${strandedId} did not finish in time; returning it to the queue`,
    );
    try {
      // Ownership-guarded: if the reaper already took it, this is a no-op.
      await pool.query(
        `UPDATE jobs
         SET status = 'pending', worker_id = NULL, lease_expires_at = NULL,
             error = 'worker shut down before the job finished',
             updated_at = now()
         WHERE id = $1 AND worker_id = $2 AND status = 'running'`,
        [strandedId, workerId],
      );
    } catch (err) {
      // Not fatal: the lease still expires and the reaper picks it up.
      console.error(`[worker ${workerId}] failed to hand back job ${strandedId}`, err);
    }
  }

  try {
    await pool.end();
  } catch {
    // ignore — the process is exiting
  }
  console.log(`[worker ${workerId}] shutdown complete`);
  process.exit(0);
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    void shutdown(signal);
  });
}

startReaper();

main().catch((err) => {
  console.error("[worker] fatal", err);
  process.exit(1);
});
