import { execSync, exec } from "child_process";
import { promisify } from "util";
import { pool } from "../src/db/client";

const execAsync = promisify(exec);

/**
 * Proves Phase 8 by actually killing workers, not by simulating it.
 *
 *   1. SIGKILL a worker holding a job → the job is reclaimed and completes
 *   2. SIGTERM a worker holding a job → it finishes or hands the job back fast,
 *      without waiting for the lease to lapse
 *   3. leases are heartbeated, so a long job is never reclaimed underneath
 *      a worker that is still alive
 *
 * Assumes the fleet is running:
 *   docker compose up -d --build --scale worker=5
 */

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
let failures = 0;

function check(label: string, ok: boolean, detail = ""): void {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${detail ? ` — ${detail}` : ""}`);
  if (!ok) failures++;
}

function sh(cmd: string): string {
  return execSync(cmd, { encoding: "utf8" }).trim();
}

/** Maps a worker_id (hostname-pid) back to its container name. */
function containerForWorker(workerId: string): string | null {
  const hostname = workerId.split("-")[0];
  const names = sh(`docker ps --format "{{.Names}}" --filter "name=worker"`)
    .split("\n")
    .filter(Boolean);

  for (const name of names) {
    const id = sh(`docker inspect -f "{{.Config.Hostname}}" ${name}`);
    if (id === hostname) return name;
  }
  return null;
}

async function enqueueLongJob(ms: number, maxAttempts = 5): Promise<number> {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO jobs (type, payload, max_attempts)
     VALUES ('sleep', jsonb_build_object('ms', $1::int), $2)
     RETURNING id`,
    [ms, maxAttempts],
  );
  return rows[0].id;
}

async function waitUntilRunning(id: number, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await pool.query<{
      status: string;
      worker_id: string | null;
      lease_expires_at: string | null;
    }>(`SELECT status, worker_id, lease_expires_at FROM jobs WHERE id = $1`, [id]);
    if (rows[0].status === "running" && rows[0].worker_id) return rows[0];
    await sleep(150);
  }
  throw new Error(`job ${id} never started running — are workers up?`);
}

async function waitForStatus(id: number, status: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await pool.query<{ status: string }>(
      `SELECT status FROM jobs WHERE id = $1`,
      [id],
    );
    if (rows[0].status === status) return true;
    await sleep(200);
  }
  return false;
}

/** The headline test: a hard kill, which runs no cleanup code whatsoever. */
async function testSigkillRecovery(): Promise<void> {
  console.log("SIGKILL a worker mid-job:");

  const id = await enqueueLongJob(60_000);
  const claimed = await waitUntilRunning(id);
  const container = containerForWorker(claimed.worker_id!);

  if (!container) {
    check("found the container holding the job", false, `worker ${claimed.worker_id}`);
    return;
  }
  console.log(`  job ${id} claimed by ${claimed.worker_id} (${container}), killing it`);

  // SIGKILL, not stop: no signal handler runs, no lease is released, nothing is
  // cleaned up. Exactly what an OOM kill or a yanked power cable looks like.
  sh(`docker kill --signal=KILL ${container}`);

  const { rows: afterKill } = await pool.query<{ status: string }>(
    `SELECT status FROM jobs WHERE id = $1`,
    [id],
  );
  check("job is stranded in running immediately after the kill", afterKill[0].status === "running",
    afterKill[0].status);

  // Lease is 30s; reaper scans every 5s. Allow generous slack.
  const reclaimed = await waitForStatus(id, "pending", 70_000);
  check("lease lapsed and the job was reclaimed to pending", reclaimed);

  const { rows: state } = await pool.query<{
    reclaim_count: number;
    attempts: number;
    worker_id: string | null;
    error: string | null;
  }>(`SELECT reclaim_count, attempts, worker_id, error FROM jobs WHERE id = $1`, [id]);

  check("reclaim_count incremented", state[0].reclaim_count >= 1, `${state[0].reclaim_count}`);
  check("worker_id cleared", state[0].worker_id === null);
  check("attempt was still charged for the crashed run", state[0].attempts >= 1,
    `${state[0].attempts} attempts`);
  console.log(`  recorded reason: ${state[0].error}`);

  // Let it be picked up again, then get it out of the way.
  await pool.query(
    `UPDATE jobs SET payload = jsonb_build_object('ms', 200), next_run_at = now() WHERE id = $1`,
    [id],
  );
  const finished = await waitForStatus(id, "completed", 60_000);
  check("the reclaimed job eventually completed on another worker", finished);

  sh(`docker compose up -d --scale worker=5 2>&1 | tail -1`);
  await sleep(8000);
}

/**
 * Graceful stop should be fast — not "wait out the lease" slow.
 *
 * Runs against a single worker. With a fleet, the handed-back job is re-claimed
 * within a poll interval and the `pending` state is too brief to observe
 * reliably; scaling to one worker makes the handback the only thing that can
 * happen, so the assertion is about the mechanism rather than a race.
 *
 * `docker stop` is issued asynchronously and polled concurrently, because
 * execSync would block this process for the whole stop and miss the transition
 * it is supposed to be timing.
 */
async function testSigtermHandback(): Promise<void> {
  console.log("\nSIGTERM a worker mid-job (single worker, so nothing re-claims it):");

  sh(`docker compose up -d --scale worker=1 2>&1 | tail -1`);
  await sleep(9000);

  const id = await enqueueLongJob(60_000);
  const claimed = await waitUntilRunning(id);
  const container = containerForWorker(claimed.worker_id!);

  if (!container) {
    check("found the container holding the job", false);
    return;
  }
  console.log(`  job ${id} claimed by ${claimed.worker_id} (${container}), sending SIGTERM`);

  const startedAt = Date.now();
  const stopping = execAsync(`docker stop -t 25 ${container}`);

  // Poll while the stop is in flight rather than after it.
  const handedBack = await waitForStatus(id, "pending", 30_000);
  const elapsedMs = Date.now() - startedAt;
  await stopping.catch(() => {});

  check("job returned to pending after SIGTERM", handedBack, `${elapsedMs}ms`);
  check(
    "handed back without waiting out the full lease",
    handedBack && elapsedMs < 30_000,
    `${elapsedMs}ms vs 30000ms lease`,
  );

  const { rows } = await pool.query<{
    error: string | null;
    worker_id: string | null;
    reclaim_count: number;
  }>(`SELECT error, worker_id, reclaim_count FROM jobs WHERE id = $1`, [id]);
  console.log(`  recorded reason: ${rows[0].error}`);
  check("worker_id cleared on handback", rows[0].worker_id === null);
  check(
    "handed back by shutdown, not by the reaper",
    rows[0].reclaim_count === 0,
    `reclaim_count=${rows[0].reclaim_count}`,
  );

  await pool.query(
    `UPDATE jobs SET status = 'completed', next_run_at = now() WHERE id = $1`,
    [id],
  );

  sh(`docker compose up -d --scale worker=5 2>&1 | tail -1`);
  await sleep(9000);
}

/** A live worker must not have its job stolen out from under it. */
async function testHeartbeatKeepsLeaseAlive(): Promise<void> {
  console.log("\nheartbeat keeps a long job's lease alive:");

  // Comfortably longer than the 30s lease, so an un-heartbeated lease would
  // certainly lapse partway through.
  const id = await enqueueLongJob(45_000);
  const claimed = await waitUntilRunning(id);
  const firstLease = new Date(claimed.lease_expires_at!).getTime();

  console.log(`  job ${id} running on ${claimed.worker_id}, watching its lease for 35s`);

  let extended = false;
  let everExpired = false;
  const deadline = Date.now() + 35_000;

  while (Date.now() < deadline) {
    const { rows } = await pool.query<{
      status: string;
      lease_expires_at: string | null;
      reclaim_count: number;
    }>(`SELECT status, lease_expires_at, reclaim_count FROM jobs WHERE id = $1`, [id]);

    if (rows[0].status === "running" && rows[0].lease_expires_at) {
      const lease = new Date(rows[0].lease_expires_at).getTime();
      if (lease > firstLease + 1000) extended = true;
      if (lease < Date.now()) everExpired = true;
    }
    await sleep(1000);
  }

  const { rows: final } = await pool.query<{ status: string; reclaim_count: number }>(
    `SELECT status, reclaim_count FROM jobs WHERE id = $1`,
    [id],
  );

  check("lease was extended beyond its original expiry", extended);
  check("lease never lapsed while the worker was alive", !everExpired);
  check("job was never reclaimed", final[0].reclaim_count === 0, `${final[0].reclaim_count}`);

  await waitForStatus(id, "completed", 30_000);
}

async function main(): Promise<void> {
  await testSigkillRecovery();
  await testSigtermHandback();
  await testHeartbeatKeepsLeaseAlive();

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
