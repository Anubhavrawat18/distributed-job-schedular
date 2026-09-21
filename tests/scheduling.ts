import { randomUUID } from "crypto";
import { pool } from "../src/db/client";
import { nextOccurrence } from "../src/scheduler/cron";

/**
 * Proves Phase 4:
 *   1. a delayed job is not executed before its time, and is executed after
 *   2. a recurring definition materialises on schedule
 *   3. concurrent schedulers never double-enqueue the same occurrence
 *   4. an invalid cron expression is rejected, not discovered at runtime
 *
 * Assumes the fleet is running:
 *   docker compose up -d --build --scale worker=5 --scale scheduler=3
 */

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
let failures = 0;

function check(label: string, ok: boolean, detail = ""): void {
  console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${detail ? ` — ${detail}` : ""}`);
  if (!ok) failures++;
}

async function testDelayedJob(): Promise<void> {
  console.log("delayed job waits for its run time:");

  const delaySeconds = 6;
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO jobs (type, payload, next_run_at)
     VALUES ('tick', '{}'::jsonb, now() + make_interval(secs => $1::double precision))
     RETURNING id`,
    [delaySeconds],
  );
  const id = rows[0].id;

  // Halfway through the delay it must still be untouched.
  await sleep((delaySeconds * 1000) / 2);
  const { rows: mid } = await pool.query<{ status: string; attempts: number }>(
    `SELECT status, attempts FROM jobs WHERE id = $1`,
    [id],
  );
  check(
    "still pending halfway through the delay",
    mid[0].status === "pending" && mid[0].attempts === 0,
    `status=${mid[0].status} attempts=${mid[0].attempts}`,
  );

  const deadline = Date.now() + 30_000;
  let final = mid[0];
  while (Date.now() < deadline) {
    const { rows: now } = await pool.query<{ status: string; attempts: number }>(
      `SELECT status, attempts FROM jobs WHERE id = $1`,
      [id],
    );
    final = now[0];
    if (final.status === "completed") break;
    await sleep(250);
  }
  check("runs once its time arrives", final.status === "completed", `got ${final.status}`);
}

async function testRecurringJob(): Promise<void> {
  console.log("\nrecurring definition materialises on schedule:");

  const name = `test-every-minute-${randomUUID().slice(0, 8)}`;

  // Every minute is the finest granularity standard 5-field cron offers. To
  // avoid waiting a full minute for the first tick, the definition is created
  // already due.
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO recurring_jobs (name, type, payload, cron_expression, timezone, next_run_at)
     VALUES ($1, 'tick', $2, '* * * * *', 'UTC', now())
     RETURNING id`,
    [name, { source: name }],
  );
  const definitionId = rows[0].id;

  const deadline = Date.now() + 30_000;
  let produced = 0;
  while (Date.now() < deadline) {
    const { rows: jobs } = await pool.query<{ n: string }>(
      `SELECT count(*) AS n FROM jobs WHERE recurring_job_id = $1`,
      [definitionId],
    );
    produced = Number(jobs[0].n);
    if (produced >= 1) break;
    await sleep(250);
  }
  check("produced a job", produced >= 1, `${produced} job(s)`);

  const { rows: def } = await pool.query<{
    next_run_at: string;
    last_enqueued_at: string | null;
  }>(`SELECT next_run_at, last_enqueued_at FROM recurring_jobs WHERE id = $1`, [
    definitionId,
  ]);
  const advanced = new Date(def[0].next_run_at).getTime() > Date.now();
  check("advanced next_run_at into the future", advanced, def[0].next_run_at);
  check("recorded last_enqueued_at", def[0].last_enqueued_at !== null);

  const { rows: provenance } = await pool.query<{
    scheduled_for: string | null;
    status: string;
  }>(
    `SELECT scheduled_for, status FROM jobs WHERE recurring_job_id = $1 ORDER BY id LIMIT 1`,
    [definitionId],
  );
  check("job records which occurrence it is", provenance[0].scheduled_for !== null);

  // 3. No duplicate occurrences, even with several schedulers racing.
  console.log("\nconcurrent schedulers do not double-enqueue:");
  const { rows: dupes } = await pool.query<{ scheduled_for: string; n: string }>(
    `SELECT scheduled_for, count(*) AS n FROM jobs
     WHERE recurring_job_id = $1
     GROUP BY scheduled_for HAVING count(*) > 1`,
    [definitionId],
  );
  check("no occurrence produced more than one job", dupes.length === 0, `${dupes.length} duplicated`);

  await pool.query(`UPDATE recurring_jobs SET enabled = false WHERE id = $1`, [
    definitionId,
  ]);
  console.log(`  (disabled "${name}" so it stops ticking)`);
}

/**
 * The single-occurrence check above is weak evidence: with one definition
 * ticking once, three schedulers may simply never have collided. This makes the
 * collision likely by dropping many definitions in as due simultaneously, so
 * all three schedulers are contending on the same set at the same moment.
 */
async function testSchedulerRaceUnderLoad(): Promise<void> {
  const definitionCount = 60;
  console.log(`\n${definitionCount} definitions due at once, 3 schedulers racing:`);

  const prefix = `race-${randomUUID().slice(0, 8)}`;
  const { rows: created } = await pool.query<{ id: number }>(
    `INSERT INTO recurring_jobs (name, type, payload, cron_expression, timezone, next_run_at)
     SELECT $1 || '-' || g, 'tick', '{}'::jsonb, '* * * * *', 'UTC', now()
     FROM generate_series(1, $2::int) g
     RETURNING id`,
    [prefix, definitionCount],
  );
  const ids = created.map((r) => r.id);

  const deadline = Date.now() + 45_000;
  let withJobs = 0;
  while (Date.now() < deadline) {
    const { rows } = await pool.query<{ n: string }>(
      `SELECT count(DISTINCT recurring_job_id) AS n FROM jobs
       WHERE recurring_job_id = ANY($1::bigint[])`,
      [ids],
    );
    withJobs = Number(rows[0].n);
    if (withJobs >= definitionCount) break;
    await sleep(250);
  }

  // Stop them ticking before asserting, so counts cannot drift mid-check.
  await pool.query(
    `UPDATE recurring_jobs SET enabled = false WHERE id = ANY($1::bigint[])`,
    [ids],
  );
  await sleep(1500);

  check(
    "every definition produced a job",
    withJobs === definitionCount,
    `${withJobs}/${definitionCount}`,
  );

  const { rows: dupes } = await pool.query<{
    recurring_job_id: number;
    scheduled_for: string;
    n: string;
  }>(
    `SELECT recurring_job_id, scheduled_for, count(*) AS n FROM jobs
     WHERE recurring_job_id = ANY($1::bigint[])
     GROUP BY recurring_job_id, scheduled_for
     HAVING count(*) > 1`,
    [ids],
  );
  check(
    "no occurrence produced a duplicate job",
    dupes.length === 0,
    `${dupes.length} duplicated`,
  );

  const { rows: spread } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM jobs WHERE recurring_job_id = ANY($1::bigint[])`,
    [ids],
  );
  console.log(`  (${spread[0].n} jobs produced from ${definitionCount} definitions)`);
}

async function testGlobalNoDuplicateOccurrences(): Promise<void> {
  console.log("\nacross every recurring job ever scheduled:");
  const { rows } = await pool.query<{ n: string }>(
    `SELECT count(*) AS n FROM (
       SELECT recurring_job_id, scheduled_for
       FROM jobs
       WHERE recurring_job_id IS NOT NULL
       GROUP BY recurring_job_id, scheduled_for
       HAVING count(*) > 1
     ) dupes`,
  );
  check("zero duplicated occurrences", Number(rows[0].n) === 0, `${rows[0].n} found`);
}

function testCronValidation(): void {
  console.log("\ncron expressions are validated up front:");

  let threw = false;
  try {
    nextOccurrence("not a cron", "UTC", new Date());
  } catch {
    threw = true;
  }
  check("invalid expression is rejected", threw);

  const from = new Date("2026-01-01T00:02:00Z");
  const next = nextOccurrence("*/5 * * * *", "UTC", from);
  check(
    "*/5 from 00:02 gives 00:05",
    next.toISOString() === "2026-01-01T00:05:00.000Z",
    next.toISOString(),
  );

  // Timezone handling is a real source of scheduling bugs, so it is asserted
  // rather than assumed: 09:00 in Kolkata is 03:30 UTC.
  const tzNext = nextOccurrence("0 9 * * *", "Asia/Kolkata", new Date("2026-01-01T00:00:00Z"));
  check(
    "0 9 * * * in Asia/Kolkata is 03:30 UTC",
    tzNext.toISOString() === "2026-01-01T03:30:00.000Z",
    tzNext.toISOString(),
  );
}

async function main(): Promise<void> {
  testCronValidation();
  await testDelayedJob();
  await testRecurringJob();
  await testSchedulerRaceUnderLoad();
  await testGlobalNoDuplicateOccurrences();

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
