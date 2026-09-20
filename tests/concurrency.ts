import { randomUUID } from "crypto";
import { pool } from "../src/db/client";

/**
 * Proves that concurrent workers never execute the same job twice.
 *
 * Assumes a worker fleet is already running, e.g.
 *   docker compose up -d --build --scale worker=5
 *
 * Tags every job it enqueues with a unique runId so it only asserts on its own
 * rows — no TRUNCATE, so it is safe to run against a database with other jobs
 * in it.
 *
 * The assertion is on job_executions, not jobs.status, because a double
 * execution is invisible in jobs.status: the second worker's UPDATE just
 * overwrites the first and the row still reads 'completed' once.
 */

const JOB_COUNT = Number(process.env.TEST_JOB_COUNT ?? 200);
const JOB_DURATION_MS = Number(process.env.TEST_JOB_DURATION_MS ?? 20);
const TIMEOUT_MS = Number(process.env.TEST_TIMEOUT_MS ?? 120_000);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main(): Promise<void> {
  const runId = randomUUID();
  console.log(`enqueueing ${JOB_COUNT} jobs (runId=${runId})`);

  const { rows: inserted } = await pool.query<{ id: number }>(
    `INSERT INTO jobs (type, payload)
     SELECT 'sleep', jsonb_build_object('ms', $1::int, 'runId', $2::text)
     FROM generate_series(1, $3::int)
     RETURNING id`,
    [JOB_DURATION_MS, runId, JOB_COUNT],
  );

  const ids = inserted.map((r) => r.id);
  const startedAt = Date.now();

  while (true) {
    const { rows } = await pool.query<{ remaining: string }>(
      `SELECT count(*) AS remaining FROM jobs
       WHERE id = ANY($1::bigint[]) AND status IN ('pending', 'running')`,
      [ids],
    );

    if (Number(rows[0].remaining) === 0) break;

    if (Date.now() - startedAt > TIMEOUT_MS) {
      console.error(
        `TIMEOUT: ${rows[0].remaining} jobs still unfinished after ${TIMEOUT_MS}ms.` +
          ` Are any workers running? (docker compose ps)`,
      );
      await pool.end();
      process.exit(1);
    }

    await sleep(250);
  }

  const elapsedMs = Date.now() - startedAt;

  const { rows: duplicates } = await pool.query<{
    job_id: number;
    executions: string;
    workers: string[];
  }>(
    `SELECT job_id, count(*) AS executions, array_agg(worker_id) AS workers
     FROM job_executions
     WHERE job_id = ANY($1::bigint[])
     GROUP BY job_id
     HAVING count(*) > 1
     ORDER BY job_id`,
    [ids],
  );

  const { rows: stats } = await pool.query<{
    total_executions: string;
    distinct_workers: string;
  }>(
    `SELECT count(*) AS total_executions,
            count(DISTINCT worker_id) AS distinct_workers
     FROM job_executions
     WHERE job_id = ANY($1::bigint[])`,
    [ids],
  );

  console.log("");
  console.log(`jobs enqueued:     ${JOB_COUNT}`);
  console.log(`executions logged: ${stats[0].total_executions}`);
  console.log(`workers involved:  ${stats[0].distinct_workers}`);
  console.log(`wall time:         ${elapsedMs}ms`);
  console.log("");

  if (Number(stats[0].distinct_workers) < 2) {
    console.warn(
      `WARNING: only ${stats[0].distinct_workers} worker took part, so this run` +
        ` did not actually test concurrency. Scale the fleet up.`,
    );
  }

  if (duplicates.length > 0) {
    console.error(`FAIL: ${duplicates.length} job(s) executed more than once`);
    for (const d of duplicates.slice(0, 10)) {
      console.error(`  job ${d.job_id}: ${d.executions}x by ${d.workers.join(", ")}`);
    }
    await pool.end();
    process.exit(1);
  }

  console.log("PASS: every job executed exactly once");
  await pool.end();
}

main().catch(async (err) => {
  console.error("test failed to run", err);
  await pool.end();
  process.exit(1);
});
