import { pool } from "../db/client";
import { config } from "../config";
import type { ClaimStrategy } from "../config";
import type { Job } from "../types/job";

// overall working of the function
// Database -> Find oldest pending job -> No job? -> return null -> if found job -> change status from pendin to running ->return that job

/**
 * The ordering key for claiming.
 *
 * Priority alone starves low-priority work: if high-priority jobs keep
 * arriving, a priority-0 job can sit pending forever, which is a liveness bug
 * rather than a fairness nicety. Aging fixes it the way OS schedulers do — a
 * job's effective priority climbs the longer it waits, so any job eventually
 * outranks the incoming stream no matter how humble it started.
 *
 * Age is measured from next_run_at, not created_at. Using created_at would let
 * a job scheduled a month in advance arrive already enormously aged and
 * immediately outrank everything — it would have been "waiting" for a month
 * without ever having been eligible. next_run_at measures time spent actually
 * waiting for a worker.
 *
 * Note the cost: with aging on, the sort key is an expression over now(), so no
 * index can be precomputed for it and Postgres must sort the eligible rows.
 * idx_jobs_claim still narrows the scan to those rows, which is the expensive
 * part; the sort is over a set that is small whenever the fleet is keeping up.
 */
function orderingClause(agingSeconds: number): string {
  if (agingSeconds <= 0) {
    return `ORDER BY priority DESC, next_run_at, created_at`;
  }
  return `ORDER BY (priority + floor(EXTRACT(EPOCH FROM (now() - next_run_at)) / ${agingSeconds})) DESC,
                   next_run_at, created_at`;
}

/**
 * Selects a claimable job id, honouring priority and per-type concurrency caps.
 *
 * The cap filter here is *approximate*: it reads the running count from this
 * transaction's snapshot, so a concurrent claim it cannot see could push the
 * type over its cap. That is deliberate and is corrected below. Its job is to
 * stop a capped type from blocking the queue head — without this filter, a
 * worker would keep picking the highest-priority job, find its type at cap, and
 * give up, while lower-priority jobs of uncapped types sat claimable behind it.
 * That is head-of-line blocking, and it is a worse failure than briefly
 * exceeding a cap.
 */
function candidateSql(agingSeconds: number, lockClause: string): string {
  return `
    SELECT j.id, j.type
    FROM jobs j
    LEFT JOIN job_type_limits l ON l.type = j.type
    WHERE j.status = 'pending'
      AND j.next_run_at <= now()
      AND (
        l.max_concurrency IS NULL
        OR (
          SELECT count(*) FROM jobs r
          WHERE r.status = 'running' AND r.type = j.type
        ) < l.max_concurrency
      )
    ${orderingClause(agingSeconds)}
    ${lockClause}
    LIMIT 1`;
}

/**
 * Concurrency-safe claim.
 *
 * FOR UPDATE locks the row the subquery picks. SKIP LOCKED is the half that
 * matters for throughput: a worker whose candidate row is already locked skips
 * to the next unlocked row instead of blocking behind the lock. Without it, N
 * workers would queue up on the same row and effectively run single-file; with
 * it, N workers claim N different jobs in parallel.
 *
 * `next_run_at <= now()` implements both retry backoff and delayed jobs: a job
 * waiting out its delay is simply invisible to this query. The attempt counter
 * is incremented here rather than in executeJob so that it is atomic with the
 * claim — a worker that crashes mid-job has still spent an attempt, which is
 * what stops a poison job from being retried forever.
 *
 * Phase 5 adds a second locking step for capped job types. The approximate cap
 * filter in candidateSql cannot be trusted on its own, because two workers can
 * both read "3 running, cap 5" from their own snapshots and both claim, landing
 * at 5 when the true count was 4. Postgres has no declarative way to say "at
 * most N rows of this type may be in state 'running'", so the invariant is
 * enforced with an advisory lock keyed on the job type: only one worker at a
 * time may be deciding for that type, and it recounts under that lock. Workers
 * claiming *different* types never contend, so the fleet does not serialise.
 */
async function claimSkipLocked(workerId: string): Promise<Job | null> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const { rows: candidates } = await client.query<{ id: number; type: string }>(
      candidateSql(
        config.worker.priorityAgingSeconds,
        "FOR UPDATE OF j SKIP LOCKED",
      ),
    );

    if (candidates.length === 0) {
      await client.query("COMMIT");
      return null;
    }

    const { id, type } = candidates[0];

    if (!(await withinCap(client, type))) {
      // Another worker won the race for the last slot. Release the row lock and
      // let the caller poll again rather than spinning here.
      await client.query("ROLLBACK");
      return null;
    }

    // The lease starts here. From this instant the worker owes a heartbeat, and
    // if it stops sending one the job is reclaimable without its cooperation.
    const { rows } = await client.query<Job>(
      `UPDATE jobs
       SET status = 'running', worker_id = $1, attempts = attempts + 1,
           lease_expires_at = now() + make_interval(secs => $3::double precision),
           updated_at = now()
       WHERE id = $2
       RETURNING *`,
      [workerId, id, config.worker.leaseSeconds],
    );

    await client.query("COMMIT");
    return rows[0] ?? null;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Exact cap check, serialised per job type.
 *
 * pg_advisory_xact_lock is keyed on the type name and released automatically
 * when the transaction ends. It is held only across a count and an update, so
 * contention is brief even for a heavily capped type.
 */
async function withinCap(
  client: { query: typeof pool.query },
  type: string,
): Promise<boolean> {
  const { rows } = await client.query<{ max_concurrency: number }>(
    `SELECT max_concurrency FROM job_type_limits WHERE type = $1`,
    [type],
  );

  if (rows.length === 0) {
    return true;
  }

  // Skipping the lock leaves only the snapshot-based filter, which two workers
  // can both pass for the same last slot. Exposed as a setting so the cap
  // breach is reproducible on demand instead of taken on trust.
  if (config.worker.capEnforcement === "approximate") {
    return true;
  }

  await client.query(`SELECT pg_advisory_xact_lock(hashtext($1))`, [
    `jobtype:${type}`,
  ]);

  const { rows: counted } = await client.query<{ running: string }>(
    `SELECT count(*) AS running FROM jobs WHERE status = 'running' AND type = $1`,
    [type],
  );

  return Number(counted[0].running) < rows[0].max_concurrency;
}

/**
 * The Phase 1 claim, kept so the bug is reproducible rather than just described.
 *
 * The SELECT and the UPDATE are separate statements with no lock held between
 * them, so two workers can both read the same row as 'pending' and both go on to
 * execute it. Run the fleet with CLAIM_STRATEGY=naive and the duplicate-execution
 * check in tests/concurrency.ts fails; switch back to skip_locked and it passes.
 */
async function claimNaive(workerId: string): Promise<Job | null> {
  // select exactly one job with pending status
  const pending = await pool.query<{ id: number }>(
    candidateSql(config.worker.priorityAgingSeconds, ""),
  );

  if (pending.rows.length === 0) {
    return null;
  }

  // The race lives in the gap between the statement above and the one below.
  // No artificial delay is needed to hit it: with 5 workers, ~48% of jobs get
  // claimed more than once.
  const claimed = await pool.query<Job>(
    `UPDATE jobs
     SET status = 'running', worker_id = $1, attempts = attempts + 1,
         lease_expires_at = now() + make_interval(secs => $3::double precision),
         updated_at = now()
     WHERE id = $2
     RETURNING *`,
    [workerId, pending.rows[0].id, config.worker.leaseSeconds],
  );

  return claimed.rows[0] ?? null;
}

export async function claimJob(
  workerId: string,
  strategy: ClaimStrategy = config.worker.claimStrategy,
): Promise<Job | null> {
  return strategy === "naive"
    ? claimNaive(workerId)
    : claimSkipLocked(workerId);
}
