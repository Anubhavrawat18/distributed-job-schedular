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
