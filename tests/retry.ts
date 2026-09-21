import { randomUUID } from "crypto";
import { pool } from "../src/db/client";
import { computeBackoffMs } from "../src/retry/backoff";

/**
 * Proves the three things Phase 3 claims:
 *   1. a transiently failing job recovers instead of dying
 *   2. a permanently failing job stops after its budget and lands in the DLQ
 *   3. the gap between attempts actually grows (backoff is real, not cosmetic)
 *
 * Assumes a worker fleet is running:
 *   docker compose up -d --build --scale worker=5
 */

const TIMEOUT_MS = Number(process.env.TEST_TIMEOUT_MS ?? 120_000);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let failures = 0;

function check(label: string, ok: boolean, detail = ""): void {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${detail ? ` — ${detail}` : ""}`);
  if (!ok) failures++;
}

async function enqueue(
  type: string,
  payload: Record<string, unknown>,
  maxAttempts: number,
): Promise<number> {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO jobs (type, payload, max_attempts)
     VALUES ($1, $2, $3) RETURNING id`,
    [type, payload, maxAttempts],
  );
  return rows[0].id;
}

async function waitForTerminal(ids: number[]): Promise<void> {
  const startedAt = Date.now();
  while (true) {
    const { rows } = await pool.query<{ remaining: string }>(
      `SELECT count(*) AS remaining FROM jobs
       WHERE id = ANY($1::bigint[]) AND status IN ('pending', 'running')`,
      [ids],
    );
    if (Number(rows[0].remaining) === 0) return;

    if (Date.now() - startedAt > TIMEOUT_MS) {
      throw new Error(
        `timed out with ${rows[0].remaining} job(s) unfinished — are workers running?`,
      );
    }
    await sleep(250);
  }
}

async function main(): Promise<void> {
  const runId = randomUUID();

  // 1. Transient failure recovers.
  const flakyId = await enqueue("flaky", { failTimes: 2, runId }, 5);
  // 2. Permanent failure exhausts its budget and dead-letters.
  const doomedId = await enqueue("fail", { message: "always broken", runId }, 3);
  // 3. Unknown type dead-letters immediately without burning retries.
  const unknownId = await enqueue("no-such-type", { runId }, 3);

  console.log(`waiting for jobs ${flakyId}, ${doomedId}, ${unknownId}...\n`);
  await waitForTerminal([flakyId, doomedId, unknownId]);

  const { rows: jobs } = await pool.query(
    `SELECT id, status, attempts, max_attempts, error FROM jobs
     WHERE id = ANY($1::bigint[])`,
    [[flakyId, doomedId, unknownId]],
  );
  const byId = new Map(jobs.map((j: any) => [Number(j.id), j]));

  const { rows: execCounts } = await pool.query<{ job_id: number; n: string }>(
    `SELECT job_id, count(*) AS n FROM job_executions
     WHERE job_id = ANY($1::bigint[]) GROUP BY job_id`,
    [[flakyId, doomedId, unknownId]],
  );
  const execs = new Map(execCounts.map((r) => [Number(r.job_id), Number(r.n)]));

  console.log("transient failure recovers:");
  const flaky = byId.get(flakyId);
  check("status is completed", flaky.status === "completed", `got ${flaky.status}`);
  check("took 3 attempts", flaky.attempts === 3, `got ${flaky.attempts}`);
  check("executed 3 times", execs.get(flakyId) === 3, `got ${execs.get(flakyId)}`);

  console.log("\npermanent failure exhausts budget:");
  const doomed = byId.get(doomedId);
  check("status is failed", doomed.status === "failed", `got ${doomed.status}`);
  check(
    "stopped at max_attempts (3)",
    doomed.attempts === 3,
    `got ${doomed.attempts}`,
  );
  check("executed exactly 3 times", execs.get(doomedId) === 3, `got ${execs.get(doomedId)}`);

  const { rows: dlq } = await pool.query(
    `SELECT job_id, attempts, final_error FROM dead_letter_jobs
     WHERE job_id = ANY($1::bigint[])`,
    [[flakyId, doomedId, unknownId]],
  );
  const dlqIds = new Set(dlq.map((d: any) => Number(d.job_id)));
  check("landed in the dead-letter queue", dlqIds.has(doomedId));
  check("recovered job did NOT land in the DLQ", !dlqIds.has(flakyId));

  console.log("\nunknown job type is not retried:");
  const unknown = byId.get(unknownId);
  check("dead-lettered", dlqIds.has(unknownId));
  check(
    "burned only 1 attempt (no pointless retries)",
    unknown.attempts === 1,
    `got ${unknown.attempts}`,
  );

  console.log("\nbackoff grows between attempts:");
  const { rows: timeline } = await pool.query<{ started_at: string }>(
    `SELECT started_at FROM job_executions
     WHERE job_id = $1 ORDER BY started_at`,
    [doomedId],
  );
  const gaps: number[] = [];
  for (let i = 1; i < timeline.length; i++) {
    gaps.push(
      new Date(timeline[i].started_at).getTime() -
        new Date(timeline[i - 1].started_at).getTime(),
    );
  }
  console.log(`  observed gaps: ${gaps.map((g) => `${g}ms`).join(", ")}`);
  check(
    "second gap is longer than the first",
    gaps.length >= 2 && gaps[1] > gaps[0],
    `${gaps[0]}ms then ${gaps[1]}ms`,
  );

  console.log("\njitter spreads retries (1000 samples, attempt 3):");
  const samples = Array.from({ length: 1000 }, () =>
    computeBackoffMs(3, { baseMs: 1000, maxMs: 30_000 }),
  );
  const min = Math.min(...samples);
  const max = Math.max(...samples);
  const distinct = new Set(samples).size;
  console.log(`  range ${min}–${max}ms across ${distinct} distinct values`);
  check("stays within [nominal/2, nominal]", min >= 2000 && max <= 4000);
  check("delays are actually spread, not constant", distinct > 500, `${distinct} distinct`);

  console.log("");
  if (failures > 0) {
    console.error(`${failures} check(s) failed`);
    await pool.end();
    process.exit(1);
  }
  console.log("all checks passed");
  await pool.end();
}

main().catch(async (err) => {
  console.error("test failed to run", err);
  await pool.end();
  process.exit(1);
});
