import { pool } from "../db/client";

/**
 * Every dashboard query lives here, and every one is bounded — by a time window,
 * a LIMIT, or an indexed grouping. The dashboard polls every couple of seconds,
 * so an unbounded query here would run hundreds of times an hour against the
 * same database the workers are claiming from.
 */

export interface Snapshot {
  takenAt: string;
  health: Health;
  statusCounts: Record<string, number>;
  byType: TypeRow[];
  workers: WorkerRow[];
  recentFailures: FailureRow[];
  recentExecutions: ExecutionRow[];
  recurring: RecurringRow[];
  queryMs: number;
}

interface Health {
  /**
   * Age of the oldest job that is due but unclaimed.
   *
   * The single most useful number on the page, and the reason queue depth alone
   * is a poor health metric: 10,000 pending jobs that are all being drained is
   * fine, while 5 pending jobs that have been waiting nine minutes means nothing
   * is claiming them. Depth measures size; this measures whether the fleet is
   * keeping up.
   */
  oldestPendingSeconds: number | null;
  dueNow: number;
  scheduledAhead: number;
  executionsLastMinute: number;
  completedLastMinute: number;
  failedLastMinute: number;
  retryingNow: number;
  deadLetterTotal: number;

  /** Running jobs whose lease has lapsed — awaiting reclaim. Should hover at 0. */
  expiredLeases: number;
  /** Jobs ever taken back from a dead worker. */
  reclaimedTotal: number;
}

interface TypeRow {
  type: string;
  pending: number;
  running: number;
  max_concurrency: number | null;
  completed_last_hour: number;
  failed_last_hour: number;
}

interface WorkerRow {
  worker_id: string;
  executions_last_5m: number;
  last_seen: string;
  current_job_id: number | null;
  current_job_type: string | null;
}

interface FailureRow {
  job_id: number;
  type: string;
  attempts: number;
  final_error: string;
  died_at: string;
}

interface ExecutionRow {
  job_id: number;
  type: string;
  worker_id: string;
  started_at: string;
  status: string;
  attempts: number;
}

interface RecurringRow {
  name: string;
  type: string;
  cron_expression: string;
  timezone: string;
  enabled: boolean;
  next_run_at: string;
  last_enqueued_at: string | null;
}

export async function getSnapshot(): Promise<Snapshot> {
  const startedAt = Date.now();

  // Issued together rather than sequentially: the dashboard's latency is then
  // one round trip rather than seven, and the pool has spare connections
  // because workers hold theirs only briefly.
  const [health, statusCounts, byType, workers, recentFailures, recentExecutions, recurring] =
    await Promise.all([
      getHealth(),
      getStatusCounts(),
      getByType(),
      getWorkers(),
      getRecentFailures(),
      getRecentExecutions(),
      getRecurring(),
    ]);

  return {
    takenAt: new Date().toISOString(),
    health,
    statusCounts,
    byType,
    workers,
    recentFailures,
    recentExecutions,
    recurring,
    queryMs: Date.now() - startedAt,
  };
}

async function getHealth(): Promise<Health> {
  const { rows } = await pool.query<Record<string, string | null>>(
    `SELECT
       (SELECT EXTRACT(EPOCH FROM (now() - min(next_run_at)))
          FROM jobs WHERE status = 'pending' AND next_run_at <= now())::text
         AS oldest_pending_seconds,
       (SELECT count(*) FROM jobs WHERE status = 'pending' AND next_run_at <= now())::text
         AS due_now,
       (SELECT count(*) FROM jobs WHERE status = 'pending' AND next_run_at > now())::text
         AS scheduled_ahead,
       (SELECT count(*) FROM job_executions WHERE started_at > now() - interval '1 minute')::text
         AS executions_last_minute,
       (SELECT count(*) FROM jobs
          WHERE status = 'completed' AND updated_at > now() - interval '1 minute')::text
         AS completed_last_minute,
       (SELECT count(*) FROM jobs
          WHERE status = 'failed' AND updated_at > now() - interval '1 minute')::text
         AS failed_last_minute,
       -- A job that has burned an attempt but is waiting to run again. High and
       -- rising here means a dependency is degraded, well before jobs start
       -- dead-lettering.
       (SELECT count(*) FROM jobs
          WHERE status = 'pending' AND attempts > 0 AND next_run_at > now())::text
         AS retrying_now,
       (SELECT count(*) FROM dead_letter_jobs)::text AS dead_letter_total,
       -- Persistently above zero means reaping is not keeping up, or every
       -- worker that could reap is itself dead.
       (SELECT count(*) FROM jobs
          WHERE status = 'running' AND lease_expires_at < now())::text
         AS expired_leases,
       (SELECT count(*) FROM jobs WHERE reclaim_count > 0)::text AS reclaimed_total`,
  );

  const r = rows[0];
  return {
    oldestPendingSeconds:
      r.oldest_pending_seconds === null ? null : Math.round(Number(r.oldest_pending_seconds)),
    dueNow: Number(r.due_now),
    scheduledAhead: Number(r.scheduled_ahead),
    executionsLastMinute: Number(r.executions_last_minute),
    completedLastMinute: Number(r.completed_last_minute),
    failedLastMinute: Number(r.failed_last_minute),
    retryingNow: Number(r.retrying_now),
    deadLetterTotal: Number(r.dead_letter_total),
    expiredLeases: Number(r.expired_leases),
    reclaimedTotal: Number(r.reclaimed_total),
  };
}

async function getStatusCounts(): Promise<Record<string, number>> {
  const { rows } = await pool.query<{ status: string; n: string }>(
    `SELECT status, count(*) AS n FROM jobs GROUP BY status`,
  );

  const counts: Record<string, number> = {
    pending: 0,
    running: 0,
    completed: 0,
    failed: 0,
  };
  for (const row of rows) counts[row.status] = Number(row.n);
  return counts;
}

async function getByType(): Promise<TypeRow[]> {
  const { rows } = await pool.query<TypeRow>(
    `SELECT j.type,
            count(*) FILTER (WHERE j.status = 'pending')::int AS pending,
            count(*) FILTER (WHERE j.status = 'running')::int AS running,
            count(*) FILTER (
              WHERE j.status = 'completed' AND j.updated_at > now() - interval '1 hour'
            )::int AS completed_last_hour,
            count(*) FILTER (
              WHERE j.status = 'failed' AND j.updated_at > now() - interval '1 hour'
            )::int AS failed_last_hour,
            l.max_concurrency
     FROM jobs j
     LEFT JOIN job_type_limits l ON l.type = j.type
     GROUP BY j.type, l.max_concurrency
     ORDER BY pending DESC, j.type
     LIMIT 25`,
  );
  return rows;
}

/**
 * A worker is "seen" through its executions, not through a heartbeat table.
 * That keeps workers stateless — nothing to register, nothing to clean up — at
 * the cost of an idle worker looking absent. Phase 8 adds real leases, which is
 * when a heartbeat starts to earn its keep.
 */
async function getWorkers(): Promise<WorkerRow[]> {
  const { rows } = await pool.query<WorkerRow>(
    `WITH recent AS (
       SELECT worker_id, count(*)::int AS executions_last_5m, max(started_at) AS last_seen
       FROM job_executions
       WHERE started_at > now() - interval '5 minutes'
       GROUP BY worker_id
     )
     SELECT r.worker_id, r.executions_last_5m, r.last_seen,
            j.id AS current_job_id, j.type AS current_job_type
     FROM recent r
     LEFT JOIN jobs j ON j.worker_id = r.worker_id AND j.status = 'running'
     ORDER BY r.executions_last_5m DESC, r.worker_id
     LIMIT 50`,
  );
  return rows;
}

async function getRecentFailures(): Promise<FailureRow[]> {
  const { rows } = await pool.query<FailureRow>(
    `SELECT d.job_id, j.type, d.attempts, d.final_error, d.died_at
     FROM dead_letter_jobs d
     JOIN jobs j ON j.id = d.job_id
     ORDER BY d.died_at DESC
     LIMIT 15`,
  );
  return rows;
}

async function getRecentExecutions(): Promise<ExecutionRow[]> {
  const { rows } = await pool.query<ExecutionRow>(
    `SELECT e.job_id, j.type, e.worker_id, e.started_at, j.status, j.attempts
     FROM job_executions e
     JOIN jobs j ON j.id = e.job_id
     ORDER BY e.started_at DESC, e.id DESC
     LIMIT 25`,
  );
  return rows;
}

async function getRecurring(): Promise<RecurringRow[]> {
  const { rows } = await pool.query<RecurringRow>(
    `SELECT name, type, cron_expression, timezone, enabled, next_run_at, last_enqueued_at
     FROM recurring_jobs
     ORDER BY enabled DESC, next_run_at
     LIMIT 25`,
  );
  return rows;
}
