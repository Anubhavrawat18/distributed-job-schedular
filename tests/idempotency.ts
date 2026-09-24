import { randomUUID } from "crypto";
import { pool } from "../src/db/client";
import { config } from "../src/config";

/**
 * Proves Phase 6:
 *   1. a job that crashes AFTER its side effect performs that effect once,
 *      not once per attempt
 *   2. the stored result is returned to the repeat execution
 *   3. duplicate enqueues collapse onto one job row
 *   4. concurrent racers on the same key still produce a single effect
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

async function waitForTerminal(ids: number[], timeoutMs = 90_000): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const { rows } = await pool.query<{ n: string }>(
      `SELECT count(*) AS n FROM jobs
       WHERE id = ANY($1::bigint[]) AND status IN ('pending', 'running')`,
      [ids],
    );
    if (Number(rows[0].n) === 0) return;
    await sleep(200);
  }
  throw new Error("timed out waiting for jobs — are workers running?");
}

/**
 * The core scenario. The handler commits a side effect and then fails, exactly
 * as a worker killed mid-job would leave things. Retries re-run the handler, so
 * without a guard the effect happens once per attempt.
 */
async function testEffectRunsOnceAcrossRetries(): Promise<void> {
  console.log("side effect survives retries without repeating:");

  const label = `charge-${randomUUID().slice(0, 8)}`;
  const maxAttempts = 4;
  const crashUntilAttempt = 2;

  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO jobs (type, payload, max_attempts)
     VALUES ('charge-then-crash', $1, $2)
     RETURNING id`,
    [{ label, crashUntilAttempt }, maxAttempts],
  );
  const id = rows[0].id;

  await waitForTerminal([id]);

  const { rows: job } = await pool.query<{ status: string; attempts: number; result: any }>(
    `SELECT status, attempts, result FROM jobs WHERE id = $1`,
    [id],
  );
  const { rows: effects } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM side_effects WHERE label = $1`,
    [label],
  );
  const { rows: execs } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM job_executions WHERE job_id = $1`,
    [id],
  );

  const effectCount = Number(effects[0].n);
  const execCount = Number(execs[0].n);

  console.log(
    `  ${execCount} executions, ${effectCount} side effect(s), final status ${job[0].status}`,
  );

  check("the job ran more than once", execCount > 1, `${execCount} executions`);

  if (config.idempotency.mode === "off") {
    check(
      "IDEMPOTENCY=off: effect repeated once per execution (the bug)",
      effectCount === execCount,
      `${effectCount} effects for ${execCount} executions`,
    );
    return;
  }

  check("the side effect happened exactly once", effectCount === 1, `${effectCount} effects`);
  check("the job eventually succeeded", job[0].status === "completed", job[0].status);
  check(
    "the repeat execution reused the stored result",
    job[0].result?.repeatedEffect === true,
    JSON.stringify(job[0].result),
  );
  check(
    "the stored result identifies the original effect",
    typeof job[0].result?.sideEffectId === "number",
    JSON.stringify(job[0].result),
  );
}

/**
 * Enqueue idempotency: the client cannot distinguish a lost request from a lost
 * response, so retrying must be safe.
 */
async function testDuplicateEnqueueCollapses(): Promise<void> {
  console.log("\nduplicate enqueues collapse onto one job:");

  const key = `enqueue-${randomUUID()}`;

  const insert = () =>
    pool.query<{ id: number }>(
      `INSERT INTO jobs (type, payload, idempotency_key)
       VALUES ('tick', '{}'::jsonb, $1)
       ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
       DO NOTHING
       RETURNING id`,
      [key],
    );

  const first = await insert();
  const second = await insert();

  check("first insert created a job", first.rows.length === 1);
  check("second insert created nothing", second.rows.length === 0);

  const { rows: total } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM jobs WHERE idempotency_key = $1`,
    [key],
  );
  check("exactly one job exists for the key", Number(total[0].n) === 1, `${total[0].n} rows`);

  // And concurrently — a read-then-write check would let both pass.
  const raceKey = `enqueue-race-${randomUUID()}`;
  const raceInsert = () =>
    pool.query<{ id: number }>(
      `INSERT INTO jobs (type, payload, idempotency_key)
       VALUES ('tick', '{}'::jsonb, $1)
       ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
       DO NOTHING
       RETURNING id`,
      [raceKey],
    );

  const results = await Promise.all(Array.from({ length: 10 }, raceInsert));
  const created = results.filter((r) => r.rows.length === 1).length;
  check("10 simultaneous enqueues created exactly 1 job", created === 1, `${created} created`);
}

/**
 * Two workers executing the same guarded effect at the same instant. Postgres
 * arbitrates: the second blocks on the first's uncommitted insert, then takes
 * the already-done path.
 */
async function testConcurrentEffectRace(): Promise<void> {
  console.log("\nconcurrent executions of the same key:");

  if (config.idempotency.mode === "off") {
    console.log("  (skipped: IDEMPOTENCY=off)");
    return;
  }

  const { withIdempotency } = await import("../src/idempotency/withIdempotency");
  const label = `race-${randomUUID().slice(0, 8)}`;
  const key = `test:${label}`;

  const attempt = () =>
    withIdempotency(key, null, async (client) => {
      const { rows } = await client.query<{ id: number }>(
        `INSERT INTO side_effects (label) VALUES ($1) RETURNING id`,
        [label],
      );
      return { sideEffectId: rows[0].id };
    });

  const outcomes = await Promise.all(Array.from({ length: 12 }, attempt));

  const executedCount = outcomes.filter((o) => o.executed).length;
  const { rows: effects } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM side_effects WHERE label = $1`,
    [label],
  );

  console.log(`  12 racers, ${executedCount} performed the effect`);
  check("exactly one racer performed the effect", executedCount === 1, `${executedCount}`);
  check("exactly one side effect row exists", Number(effects[0].n) === 1, `${effects[0].n}`);

  const ids = new Set(outcomes.map((o) => o.result?.sideEffectId));
  check("every racer saw the same result", ids.size === 1, `${ids.size} distinct results`);

  await pool.query(`DELETE FROM idempotency_records WHERE key = $1`, [key]);
}

async function main(): Promise<void> {
  console.log(`IDEMPOTENCY=${config.idempotency.mode}\n`);

  await testEffectRunsOnceAcrossRetries();
  await testDuplicateEnqueueCollapses();
  await testConcurrentEffectRace();

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
