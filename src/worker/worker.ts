import { config } from "../config";
import { pool } from "../db/client";
import { claimJob } from "./claimJob";
import { executeJob } from "./executeJob";

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

const workerId = config.worker.id;

async function main(): Promise<void> {
  console.log(
    `[worker ${workerId}] started, polling every ${config.worker.pollIntervalMs}ms, claim strategy: ${config.worker.claimStrategy}`,
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
    await executeJob(job, workerId);
    // Drain back-to-back without sleeping while work remains in the queue.
  }

  await pool.end();
  console.log("[worker] stopped");
}

// Phase 8 turns this into proper graceful shutdown (in-flight job handover,
// lease release). For now it just stops the loop after the current job.
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    console.log(`[worker] ${signal} received, finishing current job`);
    running = false;
  });
}

main().catch((err) => {
  console.error("[worker] fatal", err);
  process.exit(1);
});
