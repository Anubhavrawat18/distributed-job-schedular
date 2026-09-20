import "dotenv/config";
import { hostname } from "os";

// Phase 2: the naive strategy is kept deliberately so the double-claim race can
// be reproduced on demand, not just described. See src/worker/claimJob.ts.
export type ClaimStrategy = "skip_locked" | "naive";

const claimStrategy = (process.env.CLAIM_STRATEGY ?? "skip_locked") as ClaimStrategy;
if (claimStrategy !== "skip_locked" && claimStrategy !== "naive") {
  throw new Error(`CLAIM_STRATEGY must be "skip_locked" or "naive", got "${claimStrategy}"`);
}

export const config = {
  port: Number(process.env.PORT ?? 3000),
  databaseUrl:
    process.env.DATABASE_URL ??
    "postgres://scheduler:scheduler@localhost:5432/job_scheduler",
  worker: {
    pollIntervalMs: Number(process.env.WORKER_POLL_INTERVAL_MS ?? 1000),

    // Identifies which worker holds a job. In Compose each replica is its own
    // container, so hostname+pid is unique across the fleet.
    id: process.env.WORKER_ID ?? `${hostname()}-${process.pid}`,

    claimStrategy,
  },
};
