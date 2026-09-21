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

---

## Phase 3 — Retries, exponential backoff, and the dead-letter queue

**What:** A failed job is no longer terminal. Jobs carry a retry budget
(`attempts` / `max_attempts`, overridable per job at enqueue), a failed attempt
is rescheduled with exponentially increasing delay plus jitter, and a job that
exhausts its budget lands in a `dead_letter_jobs` table with the error that
killed it. Adds `GET /jobs/dead-letter`.

**Purpose:** Solves **transient failure**, which is the normal case in a
distributed system rather than the exception. Networks blip, dependencies
restart, rate limits trip. Phase 1 treated all of these as permanent and threw
the work away. The interesting part is not "try again" — it is trying again in a
way that does not make the original problem worse, and knowing when to stop.

**How it works:**

- **Backoff is a row value, not a timer.** A failed job goes back to `pending`
  with `next_run_at = now() + delay`, and the claim query gained
  `AND next_run_at <= now()`. A job serving its backoff is simply *invisible* to
  workers until its time comes. This is the whole mechanism — there is no timer,
  no sleeping worker, no in-memory delay queue. That matters because an
  in-memory delay is lost on restart: a worker holding 50 pending retries that
  gets redeployed drops all 50. Here a restart changes nothing, because the
  delay was never in the process to begin with.

- **The attempt counter increments at claim time**, inside the same atomic
  `UPDATE` as the claim — not when the handler finishes. If it incremented on
  completion, a worker that is killed mid-execution would never record the
  attempt, and a job that reliably crashes its worker (OOM, segfault, infinite
  loop) would be retried forever, taking down each worker that touches it in
  turn. Charging the attempt up front means a poison job exhausts its budget and
  dead-letters instead of becoming a fleet-wide outage.

- **Jitter is the point, not a detail** (`src/retry/backoff.ts`). Exponential
  growth alone gives every job a *deterministic* delay: if a dependency goes
  down and 1000 jobs fail within the same second, all 1000 compute the same
  backoff, wake at the same instant, and stampede the service the moment it
  recovers — re-triggering the outage they were retrying because of. Jitter
  smears the retries across a window so recovery is gradual.

  The flavour here is **equal jitter** (`half + random(half)`) rather than AWS's
  **full jitter** (`random(0, exponential)`). Full jitter spreads load marginally
  better, but its lower bound is zero, so it can reschedule a retry almost
  immediately and defeat the backoff entirely for an unlucky job. Equal jitter
  guarantees a floor of `exponential / 2` while still breaking up the herd — the
  delay is both genuinely increasing *and* spread.

- **An unknown job type skips retries entirely.** Retrying cannot make a handler
  appear, so it dead-letters on the first attempt. Distinguishing "retryable" from
  "hopeless" is what stops the DLQ from being noise.

- **The DLQ references jobs rather than moving rows into it.** The classic
  implementation relocates the row to a separate table, but that would orphan
  the job's `job_executions` history — precisely the forensic trail you want when
  investigating why something died. Keeping the job in place means "what happened
  to job 7" stays answerable, and replaying a dead job becomes an `UPDATE`
  (reset status, clear `attempts`) instead of a migration back across tables.

**Measured results** (`npm run test:retry`, 5 workers, `RETRY_BASE_MS=1000`):

| Scenario | Attempts | Final state | In DLQ? |
|---|---|---|---|
| `flaky` job failing twice, budget 5 | 3 | `completed` | no |
| `fail` job, budget 3 | 3 | `failed` | yes |
| unknown job type, budget 3 | 1 | `failed` | yes |

Observed gaps between attempts of the doomed job: **642ms, then 1733ms** —
backoff visibly growing, each within its equal-jitter band (500–1000ms, then
1000–2000ms). Across 1000 samples of attempt 3, delays spanned **2001–3998ms
over 780 distinct values**: bounded as designed, and spread rather than
constant.

The Phase 2 concurrency test still passes unchanged (200 jobs, 200 executions,
0 duplicates), confirming the rewritten claim query did not reopen the race.

**One bug worth recording:** the enqueue query originally used
`COALESCE($3, $4)` for the retry budget. Postgres could not infer a type for a
`null` parameter there and rejected the statement with `42804`, but only on the
API path — the tests insert via SQL directly and never hit it. Fixed with
explicit `::int` casts. A reminder that parameter type inference is not
guaranteed just because the column type is known.

**Running it:**

```bash
docker compose up -d --build --scale worker=5
npm run db:init          # adds attempts/max_attempts/next_run_at + DLQ table
npm run test:retry
curl localhost:3000/jobs/dead-letter
```

```bash
curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"flaky","payload":{"failTimes":2},"maxAttempts":5}'
```

**Still open:** replaying a dead-lettered job is a manual `UPDATE` — there is no
endpoint for it yet. And the Phase 2 gap remains: a job whose worker dies stays
`running` forever, since `next_run_at` only governs jobs that made it back to
`pending`. Both the stuck-job lease and crash recovery are Phase 8.

---

## Phase 4 — Scheduling: delayed jobs and cron recurring jobs

**What:** `POST /jobs` accepts `runAt` or `delaySeconds`. A new `recurring_jobs`
table holds cron *definitions*, and a separate scheduler process
(`src/scheduler/scheduler.ts`) materialises them into real job rows as each
occurrence comes due. Adds `POST/GET/PATCH /recurring-jobs`, and a `scheduler`
service in Compose that is safe to scale.

**Purpose:** Separates *when work should happen* from *when a worker is free*.
Until now a job was eligible the instant it was inserted; the queue could only
answer "as soon as possible". Delayed and recurring work is most of what a real
scheduler is asked for — retry-after-an-hour, nightly reports, hourly digests.

**How it works:**

- **Delayed jobs needed no new column, and that is the interesting part.**
  `next_run_at` already existed for retry backoff, and the claim query already
  filtered on it. "Run this in an hour" and "retry this in an hour" are the same
  statement about eligibility, so a delayed job is just a job whose *first*
  `next_run_at` is in the future. CLAUDE.md specified a `run_at` column; I
  reused `next_run_at` instead, because two columns answering "when may this
  run" would force the claim query to consult both and create a second source of
  truth to keep consistent. **Flagging it since it deviates from the plan** — the
  alternative case is that `run_at` (immutable intent) and `next_run_at` (current
  eligibility, mutated by retries) are genuinely different facts, and splitting
  them would preserve "what was originally asked for" after a retry overwrites
  it. I judged that not worth a second column yet.

- **A recurring job is a definition, not a job.** The scheduler reads
  `recurring_jobs` and inserts concrete rows into `jobs`. Keeping them separate
  means each occurrence gets its own status, attempts, retries and execution
  history — "did last night's report run?" stays answerable per night instead of
  collapsing into one perpetually-rerun row.

- **Claiming a definition uses the same `FOR UPDATE SKIP LOCKED` discipline as
  claiming a job**, and `next_run_at` is advanced inside that same transaction,
  so the definition stops being due the moment it is claimed. The failure mode
  is worse here than for jobs: a duplicated *job* wastes one execution, whereas a
  duplicated *schedule tick* silently doubles a recurring workload forever.

- **A unique index backstops the lock.** `(recurring_job_id, scheduled_for)` is
  unique, so one occurrence can produce at most one job — enforced by Postgres,
  not by the scheduler being careful. The lock should already make duplicates
  impossible; the index makes them impossible even if that logic is wrong, a
  scheduler is rolled back to an older build, or someone inserts by hand.
  Correctness that rests only on application code holding is correctness you
  cannot prove.

- **Missed occurrences are dropped, not backfilled.** If the scheduler is down
  for an hour and a job runs every minute, advancing occurrence-by-occurrence
  would enqueue 60 jobs the moment it recovers — a self-inflicted thundering
  herd where the recovery is worse than the outage. Instead the scheduler fires
  the missed tick once and advances from *now*. The tradeoff is explicit and is
  the right default for "send an hourly digest" and the wrong one for "bill every
  customer monthly" — billing should derive its period from data, not from the
  scheduler having been alive.

- **Cron expressions are validated at creation time**, so a typo returns a 400
  from `POST /recurring-jobs` rather than being discovered by a crashing
  scheduler at 3am.

**Measured results** (`npm run test:scheduling`, 5 workers, **3 schedulers**):

| Check | Result |
|---|---|
| Delayed job halfway through its delay | still `pending`, 0 attempts |
| Delayed job after its run time | `completed` |
| Recurring definition produces a job, advances `next_run_at` | yes |
| **60 definitions due at once, 3 schedulers racing** | **60 jobs, 0 duplicates** |
| All recurring occurrences ever scheduled | 0 duplicates |
| `*/5` from 00:02 | 00:05 |
| `0 9 * * *` in `Asia/Kolkata` | 03:30 UTC |

The 60-definitions case exists because the single-occurrence check was weak
evidence: with one definition ticking once, three schedulers may simply never
have collided. Dropping 60 in as due simultaneously forces contention.

The unique index was verified by **attempting the violation directly** rather
than assuming it holds — inserting the same `(recurring_job_id, scheduled_for)`
twice is rejected with `23505`, confirming the backstop is real and not
decorative. Phases 2 and 3 still pass unchanged.

**One design bug caught while building:** `nextOccurrence` initially lived in
`scheduler.ts`, and the API imported it to validate cron expressions. That
import would have executed the scheduler's module-level `main()` *inside the API
process*, silently giving every API replica its own scheduler loop — a
duplicate-tick source that the row lock would have hidden but the unique index
would have caught. Moved to `src/scheduler/cron.ts`. A reminder that in
CommonJS, importing a name from a module runs that whole module.

**Running it:**

```bash
docker compose up -d --build --scale worker=5 --scale scheduler=3
npm run db:init
npm run test:scheduling
```

```bash
curl -X POST localhost:3000/jobs -H 'content-type: application/json' \
  -d '{"type":"tick","delaySeconds":30}'

curl -X POST localhost:3000/recurring-jobs -H 'content-type: application/json' \
  -d '{"name":"nightly-report","type":"tick","cron":"0 9 * * *","timezone":"Asia/Kolkata"}'
```

**Still open:** there is no endpoint to delete a recurring definition (only
disable, since deleting sets `recurring_job_id` to NULL on every job it ever
produced and loses that provenance). Priority and per-type rate limits are
Phase 5 — right now a flood of scheduled jobs competes with interactive ones on
equal terms, which is exactly the problem priority ordering exists to solve.
