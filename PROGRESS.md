# Progress log

A running build log for the distributed job scheduler. One entry per phase,
written to explain the reasoning — not just to record the diff.

---

## Phase 1 — Basic queue: schema, enqueue API, single worker polling loop

**What:** The minimum viable durable queue. A `jobs` table in Postgres, an
Express API to enqueue and inspect jobs (`POST /jobs`, `GET /jobs/:id`,
`GET /jobs`), and a single worker process that polls for pending work,
executes it, and writes the outcome back. Docker Compose brings up Postgres;
`npm run db:init` applies `src/db/schema.sql`.

**Purpose:** Solves **durability** — the property that makes a queue a queue
rather than an array. A job that has been accepted must survive the process
that accepted it. Because the enqueue path is a plain `INSERT` and Postgres is
the source of truth, a `201` response means the job is on disk and committed:
the API can crash immediately afterwards and the job is still there for a
worker to pick up. Nothing lives in application memory, so there is no window
where accepted work can silently vanish.

It also establishes the **state machine** every later phase builds on:
`pending → running → (completed | failed)`, enforced by a `CHECK` constraint so
the database rejects invalid states rather than trusting application code to
get it right.

**How it works:**

- **Schema** (`src/db/schema.sql`) — `jobs` holds `type`, a `JSONB` `payload`,
  `status`, plus `result`/`error` for the outcome and `created_at`/`updated_at`
  for ordering and observability. `JSONB` keeps the queue generic: adding a new
  job type needs a handler, not a migration. An index on `(status, created_at)`
  backs the only hot read path — "oldest pending job" — so polling stays an
  index scan instead of degrading into a sequential scan as completed jobs
  accumulate.

- **Enqueue** (`src/api/routes/jobs.ts`) — validates `type`/`payload` and does a
  single `INSERT ... RETURNING *`, so the caller gets the durable row (including
  its server-assigned id) in one round trip. Queries are parameterised
  throughout; the `pg` driver never receives interpolated SQL.

- **Worker loop** (`src/worker/worker.ts`) — claim, execute, repeat. When the
  queue is empty it sleeps for `WORKER_POLL_INTERVAL_MS` (default 1s); when work
  is available it drains back-to-back without sleeping, so throughput is not
  capped at one job per poll interval. Polling is the deliberate choice here
  over `LISTEN/NOTIFY`: it is crash-safe by construction (a restarted worker
  simply resumes finding pending rows), whereas notifications are fire-and-forget
  and a worker that is down when one fires misses it permanently. Polling costs
  one cheap indexed query per second; correctness is worth more than that.

- **Claiming** (`src/worker/claimJob.ts`) — `SELECT` the oldest pending id, then
  `UPDATE` it to `running`. **This is knowingly the naive version.** The read
  and the write are separate statements with no lock held between them, so two
  workers can both read the same row and both execute the job. Phase 1 is
  single-worker, so the race cannot fire yet. Writing it this way first is
  intentional: it makes the Phase 2 fix (`SELECT ... FOR UPDATE SKIP LOCKED`) a
  concrete before/after rather than an unexplained incantation.

- **Execution** (`src/worker/executeJob.ts`) — dispatches on `job.type` through
  a handler registry (`sleep` and `fail` ship as observable stand-ins for real
  work). Success writes `status = 'completed'` with the handler's return value in
  `result`; a thrown error, or an unrecognised job type, writes
  `status = 'failed'` with the message in `error`. Failure is **terminal** in
  this phase — there are no retries, which is exactly the gap Phase 3 fills.

**Verified against a real Postgres:** a `sleep` job runs and stores
`result = {"sleptMs": 300}` with `status = 'completed'`; a `fail` job stores its
message in `error` with `status = 'failed'`; an unregistered job type fails with
a clear message rather than hanging in `running` forever; a job enqueued while
the worker is idle is picked up within one poll interval; malformed enqueues and
unknown ids return 400/404 instead of 500.

**Deliberately deferred:** concurrency-safe claiming (Phase 2), retries/backoff
and a dead-letter queue (Phase 3), `run_at` and cron scheduling (Phase 4),
priority and rate limiting (Phase 5), idempotency keys (Phase 6), the dashboard
(Phase 7), and real graceful shutdown with stuck-job leases (Phase 8). The
current `SIGTERM` handler stops the loop after the in-flight job but does not
yet hand off or release a lease.

**Running it:**

```bash
docker compose up -d      # Postgres on :5432
npm install
npm run db:init           # apply src/db/schema.sql (idempotent)
npm run dev:api           # terminal 1
npm run dev:worker        # terminal 2
```

The schema is applied by an explicit script rather than mounted into
`/docker-entrypoint-initdb.d`: a bind-mounted single file needs a Docker Desktop
file-sharing grant on Windows, and an entrypoint script only ever runs on a
brand-new volume. `CREATE TABLE IF NOT EXISTS` makes the script safe to re-run
against an existing database.

```bash
curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"sleep","payload":{"ms":500}}'
curl localhost:3000/jobs/1
```

---

## Phase 2 — Concurrency-safe job claiming

**What:** Workers now claim jobs with a single atomic
`UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1)` instead of a
separate `SELECT` then `UPDATE`. Added `jobs.worker_id` (who holds a job) and a
`job_executions` table (one row per execution attempt), a worker `Dockerfile` so
the fleet scales via `docker compose up --scale worker=N`, and a test that
measures duplicate execution directly.

**Purpose:** Prevent two workers from claiming and executing the same job. This
is the core correctness property of a distributed queue — if a job charges a
card or sends an email, running it twice is a production incident, not a
performance nit.

**How it works:**

The Phase 1 claim had a gap between reading a row and writing it:

```
worker A: SELECT ... → sees job 7 pending
worker B: SELECT ... → sees job 7 pending   (A has not written yet)
worker A: UPDATE 7 → running
worker B: UPDATE 7 → running                 both now execute job 7
```

Nothing held a lock across those two statements, so the read was stale by the
time the write landed. The fix collapses claim into one statement, where the row
lock is taken and the write happens inside the same transaction:

```sql
UPDATE jobs
SET status = 'running', worker_id = $1, updated_at = now()
WHERE id = (
  SELECT id FROM jobs
  WHERE status = 'pending'
  ORDER BY created_at
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING *;
```

`FOR UPDATE` locks the row the subquery selects. **`SKIP LOCKED` is the half
that matters for throughput:** a worker whose candidate row is already locked
skips to the next unlocked row rather than blocking on it. Without `SKIP
LOCKED`, `FOR UPDATE` alone would still be _correct_ — no double execution — but
every worker would pile up waiting on the same oldest row and the fleet would
degrade to serial execution. `SKIP LOCKED` is what makes N workers pull N
_different_ jobs concurrently. `LIMIT 1` sits inside the subquery so each worker
locks exactly one row.

**Why `job_executions` instead of asserting on `jobs.status`:** a double
execution is _invisible_ in the jobs row. If two workers run job 7, the second
worker's `UPDATE ... SET status = 'completed'` simply overwrites the first, and
the row still reads `completed` exactly once — the duplicate work leaves no
trace. An append-only table cannot be overwritten, so
`GROUP BY job_id HAVING count(*) > 1` is a direct measurement of duplicate
execution rather than a proxy for it. It also becomes the execution history the
Phase 7 dashboard needs.

**Measured results** (200 jobs, 20ms each, 5 worker containers,
`tests/concurrency.ts`):

| Configuration            | Executions for 200 jobs | Duplicates        | Wall time |
| ------------------------ | ----------------------- | ----------------- | --------- |
| `skip_locked`, 5 workers | 200                     | 0                 | 1816ms    |
| `naive`, 5 workers       | 316                     | 96 jobs (some 3×) | 2862ms    |
| `skip_locked`, 1 worker  | 200                     | 0                 | 6274ms    |

Two things worth noting. First, the race is not theoretical or rare: the naive
claim double-ran **~48% of jobs** with no artificial delay inserted — it needed
no help to fail. Second, 5 workers finish in 1816ms versus 6274ms for one, a
**3.5× speedup**, which is the `SKIP LOCKED` half of the story: correctness that
does not cost parallelism.

The naive strategy is kept behind `CLAIM_STRATEGY=naive` rather than deleted, so
the failure is reproducible on demand instead of merely documented. Flipping the
env var and re-running the test turns the table above into something you can
regenerate in 30 seconds.

**Running it:**

```bash
docker compose up -d --build --scale worker=5
npm run db:init            # adds worker_id + job_executions (idempotent)
npm run test:concurrency   # PASS: every job executed exactly once
```

```bash
CLAIM_STRATEGY=naive docker compose up -d --scale worker=5
npm run test:concurrency   # FAIL: N job(s) executed more than once
```

**Still open:** a worker that dies mid-execution leaves its job stuck in
`running` forever — nothing reclaims it. `worker_id` is the hook for that fix,
but the lease/heartbeat logic is Phase 8. Retries (Phase 3) are also still
missing, so a failed job remains terminal.
