import { config } from "../config";
import { pool } from "../db/client";
import { nextOccurrence } from "./cron";
import type { RecurringJob } from "../types/job";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let running = true;
const schedulerId = config.scheduler.id;

/**
 * Claims one due recurring definition and advances it to its next occurrence,
 * atomically.
 *
 * Same SKIP LOCKED discipline as claiming a job, and for the same reason: with
 * more than one scheduler replica, a plain read-then-write would let both see
 * the same definition as due and both enqueue it. The difference is that a
 * duplicate *job* wastes work, whereas a duplicate *schedule tick* silently
 * doubles a recurring workload forever.
 *
 * The advance happens here rather than after enqueueing so the definition stops
 * being due the instant it is claimed.
 */
async function claimDueDefinition(): Promise<{
  definition: RecurringJob;
  scheduledFor: Date;
} | null> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const { rows } = await client.query<RecurringJob>(
      `SELECT * FROM recurring_jobs
       WHERE enabled AND next_run_at <= now()
       ORDER BY next_run_at
       FOR UPDATE SKIP LOCKED
       LIMIT 1`,
    );

    if (rows.length === 0) {
      await client.query("COMMIT");
      return null;
    }

    const definition = rows[0];
    const scheduledFor = new Date(definition.next_run_at);

    // Catch-up policy: advance from *now*, not from the occurrence we just
    // claimed. If the scheduler was down for an hour and this runs every
    // minute, advancing occurrence-by-occurrence would enqueue 60 jobs at once
    // — a self-inflicted thundering herd, where the recovery is worse than the
    // outage. Firing the missed tick once and then jumping to the next future
    // occurrence keeps recovery proportional to the schedule, not to the
    // downtime. The tradeoff is explicit: missed occurrences are dropped, not
    // backfilled, which is the right default for "send a digest every hour"
    // and the wrong one for "bill every customer monthly" — the latter should
    // derive its period from data, not from having been running.
    const advanceFrom = new Date(
      Math.max(scheduledFor.getTime(), Date.now()),
    );
    const nextRunAt = nextOccurrence(
      definition.cron_expression,
      definition.timezone,
      advanceFrom,
    );

    await client.query(
      `UPDATE recurring_jobs
       SET next_run_at = $2, last_enqueued_at = now(), updated_at = now()
       WHERE id = $1`,
      [definition.id, nextRunAt],
    );

    await client.query("COMMIT");
    return { definition, scheduledFor };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function materialise(
  definition: RecurringJob,
  scheduledFor: Date,
): Promise<void> {
  // ON CONFLICT DO NOTHING pairs with the unique index on
  // (recurring_job_id, scheduled_for): if this occurrence somehow already
  // exists, the insert is a no-op instead of an error.
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO jobs (type, payload, max_attempts, next_run_at, recurring_job_id, scheduled_for)
     VALUES ($1, $2, $3, $4, $5, $4)
     ON CONFLICT (recurring_job_id, scheduled_for) WHERE recurring_job_id IS NOT NULL
     DO NOTHING
     RETURNING id`,
    [
      definition.type,
      definition.payload,
      definition.max_attempts,
      scheduledFor,
      definition.id,
    ],
  );

  if (rows.length === 0) {
    console.warn(
      `[scheduler ${schedulerId}] occurrence ${scheduledFor.toISOString()} of "${definition.name}" already existed, skipped`,
    );
    return;
  }

  console.log(
    `[scheduler ${schedulerId}] enqueued job ${rows[0].id} from "${definition.name}" for ${scheduledFor.toISOString()}`,
  );
}

async function main(): Promise<void> {
  console.log(
    `[scheduler ${schedulerId}] started, polling every ${config.scheduler.pollIntervalMs}ms`,
  );

  while (running) {
    let claimed;
    try {
      claimed = await claimDueDefinition();
    } catch (err) {
      console.error(`[scheduler ${schedulerId}] claim failed`, err);
      await sleep(config.scheduler.pollIntervalMs);
      continue;
    }

    if (!claimed) {
      await sleep(config.scheduler.pollIntervalMs);
      continue;
    }

    try {
      await materialise(claimed.definition, claimed.scheduledFor);
    } catch (err) {
      // The definition has already been advanced, so a failed insert means this
      // occurrence is skipped rather than retried. Logged loudly because it is
      // silent data loss otherwise.
      console.error(
        `[scheduler ${schedulerId}] failed to enqueue occurrence of "${claimed.definition.name}"`,
        err,
      );
    }
  }

  await pool.end();
  console.log(`[scheduler ${schedulerId}] stopped`);
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    console.log(`[scheduler ${schedulerId}] ${signal} received, stopping`);
    running = false;
  });
}

main().catch((err) => {
  console.error(`[scheduler ${schedulerId}] fatal`, err);
  process.exit(1);
});
