# Distributed Job Scheduler

A durable, concurrency-safe job queue built from first principles on PostgreSQL
and TypeScript — no BullMQ, no RabbitMQ, no Celery, no Airflow.

Postgres is the source of truth, not Redis. That choice is the point: it means
locking, durability, backoff, leases and idempotency have to be **solved
explicitly in SQL** rather than inherited for free from a library. Every
mechanism here is one you can open, read, and defend.

```
POST /jobs ──▶  ┌──────────────┐  ◀── scheduler ── cron definitions
                │              │      (materialises due occurrences)
                │   Postgres   │
                │    `jobs`    │  ◀── worker 1 ─┐
                │              │  ◀── worker 2 ─┤ FOR UPDATE SKIP LOCKED
GET /api/stats ─┤  (the queue) │  ◀── worker N ─┘ + heartbeated leases
   (dashboard)  └──────────────┘
```

---

## Start here

If you only read three things:

| File | Why |
|---|---|
| **[`src/worker/claimJob.ts`](src/worker/claimJob.ts)** | The heart of the system. Contains *both* the correct `FOR UPDATE SKIP LOCKED` claim and the naive `SELECT`-then-`UPDATE` one, side by side behind a config flag — so the double-claim race is **reproducible on demand**, not just described in a comment. Also holds the priority-aging sort and the two-stage concurrency-cap check. |
| **[`src/idempotency/withIdempotency.ts`](src/idempotency/withIdempotency.ts)** | Why exactly-once execution is unachievable, and what to build instead: at-least-once delivery plus effects that are safe to repeat, with the guard record and the effect committed in a single transaction. |
| **[`PROGRESS.md`](PROGRESS.md)** | The reasoning log. One entry per phase: what was built, which distributed-systems problem it solves, and why the alternatives were rejected. Written to be explained out loud. |

---

## Every failure mode is reproducible, not just described

It is easy to write a comment saying "without `SKIP LOCKED`, two workers would
double-claim." This project makes you able to *watch it happen*. The broken
implementation is kept next to the correct one and selected by an environment
variable, so each claim can be demonstrated against a live fleet:

| Set this | And this breaks, on purpose |
|---|---|
| `CLAIM_STRATEGY=naive` | Two workers claim the same job. With 5 workers, ~48% of jobs get executed more than once. |
| `CAP_ENFORCEMENT=approximate` | The snapshot-based cap filter alone lets two workers both take the last slot, so a per-type cap is breached. |
| `IDEMPOTENCY=off` | A job that crashes after its side effect repeats that effect once per attempt. |
| `PRIORITY_AGING_SECONDS=0` | A low-priority job starves indefinitely under a steady stream of high-priority work. |

Flip each back to its default and the corresponding test passes. That symmetry
— break it, prove it breaks, fix it, prove it is fixed — is the design
principle of the whole repo.

---

## Quickstart

Requires Docker and Node 20+.

```bash
npm install
cp .env.example .env
docker compose up -d postgres        # Postgres on :5432
npm run db:init                      # apply schema.sql + migrations/
docker compose up -d --build --scale worker=5 --scale scheduler=2
npm run dev:api                      # API + dashboard on :3000
```

Enqueue a job and watch it run:

```bash
curl -X POST localhost:3000/jobs -H "content-type: application/json" -d "{\"type\":\"sleep\",\"payload\":{\"ms\":500},\"priority\":5}"
```

```bash
curl localhost:3000/jobs/1
```

Open **http://localhost:3000** for the live dashboard: queue depth by status,
throughput, in-flight jobs by worker, recent failures, and the dead-letter
queue.

---

## Proof

Each phase ships an executable proof that runs against a **real running fleet**
— real containers, real contention, real `SIGKILL` — not mocks. Bring the fleet
up first, then:

```bash
npm run test:concurrency   # concurrent workers never execute the same job twice
npm run test:retry         # transient failures recover, permanent ones dead-letter, backoff actually grows
npm run test:scheduling    # delayed jobs wait, cron materialises, concurrent schedulers never double-enqueue
npm run test:priority      # priority ordering holds, aging rescues starved jobs, caps are never exceeded
npm run test:idempotency   # a crash after a side effect performs it once, not once per attempt
npm run test:recovery      # SIGKILL a worker mid-job → reclaimed; SIGTERM → handed back; leases heartbeat
```

One detail worth calling out: `test:concurrency` asserts on an append-only
`job_executions` table, **not** on `jobs.status`. A double execution is
invisible in `jobs.status` — the second worker's `UPDATE` simply overwrites the
first, and the row still reads `completed` exactly once. Measuring the obvious
thing would have made the broken implementation look correct.

---

## What is built, and what it solves

| Phase | Mechanism | Distributed-systems problem |
|---|---|---|
| 1 | `jobs` table, `INSERT`-based enqueue, polling worker, `CHECK`-constrained state machine | **Durability** — a `201` means the job is committed to disk, so an API crash cannot lose accepted work |
| 2 | `SELECT … FOR UPDATE SKIP LOCKED` inside a transaction | **Concurrency safety** — N workers claim N different jobs in parallel instead of queueing single-file behind one row |
| 3 | Exponential backoff with jitter, per-job retry budget, `dead_letter_jobs` | **Transient vs. permanent failure** — recover from the first, stop paying for the second; jitter prevents retry thundering herds |
| 4 | `next_run_at` gating, `recurring_jobs` + cron parsing, a separate scheduler process | **Time** — delayed and recurring work, where concurrent schedulers still materialise each occurrence exactly once |
| 5 | Priority ordering with **aging**, per-type concurrency caps via advisory locks | **Fairness and isolation** — priority without starvation; one job type cannot monopolise the fleet |
| 6 | `Idempotency-Key` on enqueue, `ctx.idempotent()` for side effects | **Duplicate work** — at-least-once delivery plus repeat-safe effects, since exactly-once is not achievable |
| 7 | `GET /api/stats` and a zero-build polling dashboard | **Observability** — queue depth, throughput, failures and the DLQ, visible without a query console |
| 8 | Heartbeated leases, a reaper inside every worker, `SIGTERM` draining | **Crash recovery** — a killed worker's job is reclaimed without its cooperation, and no single process's death stops recovery |

Full reasoning for each phase lives in [`PROGRESS.md`](PROGRESS.md).

---

## Design decisions worth defending

**Why Postgres and not Redis.** Redis hands you atomic list operations for
free, which is exactly the part that makes this a worthwhile exercise. Postgres
also gives the queue transactional integrity with the business data around it:
a job can be enqueued in the same transaction as the row change that justifies
it, so there is no window where one committed and the other did not.

**Aging is measured from `next_run_at`, not `created_at`.** Using `created_at`
would let a job scheduled a month in advance arrive already enormously aged and
immediately outrank everything — it was never actually *waiting*, merely not
yet eligible.

**The concurrency-cap check is deliberately two-stage.** The in-query filter is
approximate because it reads from the transaction's own snapshot. Its real job
is to stop a capped type from blocking the queue head — head-of-line blocking
is a worse failure than briefly exceeding a cap. The exact invariant is then
enforced under a `pg_advisory_xact_lock` keyed on the job type, so workers
claiming *different* types never contend with each other.

**`attempts` is incremented at claim time, not at completion.** It is atomic
with the claim, so a worker that crashes mid-job has still spent an attempt —
which is what stops a poison job from being retried forever.

**The idempotency guard and the side effect share one transaction.** Split them
in either order and a crash between the two either loses the effect forever or
repeats it — precisely the bug the guard exists to prevent.

**Every worker runs the reaper.** Recovery has no single point of failure:
there is no one process whose death would leave crashed workers' jobs stranded.

---

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/jobs` | Enqueue. Takes `type`, `payload`, plus optional `priority`, `maxAttempts`, `runAt` / `delaySeconds`, `idempotencyKey` (or an `Idempotency-Key` header). Returns `201` for a new job, `200` with the original job for an idempotent replay. |
| `GET` | `/jobs/:id` | Status, result or error, attempt count, owning worker |
| `GET` | `/jobs?status=&limit=` | Filtered list, newest first |
| `GET` | `/jobs/dead-letter` | Jobs that exhausted their retry budget, with the final error |
| `POST` | `/recurring-jobs` | Register a cron definition — validated at registration, not discovered at runtime |
| `GET` | `/recurring-jobs` | List definitions |
| `PATCH` | `/recurring-jobs/:id` | Enable or disable; disabling preserves the provenance of jobs already produced |
| `GET` | `/job-type-limits` | Current caps alongside live running counts |
| `PUT` | `/job-type-limits/:type` | Set a per-type concurrency cap (upsert) |
| `DELETE` | `/job-type-limits/:type` | Remove a cap |
| `GET` | `/api/stats` | Full queue snapshot — the dashboard's only data source |
| `GET` | `/health` | Liveness, including database reachability |

The built-in job handlers in [`src/worker/executeJob.ts`](src/worker/executeJob.ts)
exist to make queue behaviour observable: `sleep`, `tick`, `fail`, `flaky`
(fails N times, then succeeds) and `charge-then-crash` (commits a side effect,
then dies — the precise shape of the bug idempotency fixes).

---

## Layout

```
src/
├── db/             schema.sql, incremental migrations/, pg pool
├── api/            Express server + routes (jobs, recurring, limits, stats)
├── worker/         polling loop, claimJob.ts, executeJob.ts
├── scheduler/      materialises delayed and cron-driven work
├── retry/          exponential backoff + jitter
├── idempotency/    at-most-once side effects
├── recovery/       lease reaper for crashed workers
└── observability/  the stats snapshot query
tests/              executable proofs, run against a live fleet
dashboard/          a single static HTML page, no build step
```

---

## Configuration

Defaults live in [`src/config.ts`](src/config.ts), each overridable by
environment variable; see [`.env.example`](.env.example).

| Variable | Default | Notes |
|---|---|---|
| `WORKER_POLL_INTERVAL_MS` | `1000` | How often an idle worker looks for work |
| `CLAIM_STRATEGY` | `skip_locked` | `naive` reproduces the double-claim race |
| `CAP_ENFORCEMENT` | `exact` | `approximate` reproduces the cap breach |
| `IDEMPOTENCY` | `enforced` | `off` reproduces the repeated side effect |
| `PRIORITY_AGING_SECONDS` | `60` | Effective priority rises by 1 per interval waited; `0` disables aging |
| `LEASE_SECONDS` / `HEARTBEAT_INTERVAL_MS` | `30` / `10000` | Two missed beats of slack before a worker is presumed dead |
| `REAP_INTERVAL_MS` | `5000` | How often each worker scans for abandoned jobs |
| `SHUTDOWN_GRACE_MS` | `15000` | `SIGTERM` drain window; must stay below Compose's `stop_grace_period` |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_MS` / `RETRY_MAX_MS` | `3` / `1000` / `30000` | Default retry budget, overridable per job |

---

## Known limits

Stated plainly, because a queue that claims no limits has not been thought
about hard enough:

- **Polling, not `LISTEN`/`NOTIFY`.** Latency is bounded below by the poll
  interval. `NOTIFY` would cut idle latency, but polling is what survives a
  dropped connection without extra reconnection machinery.
- **Throughput is bounded by Postgres.** This design trades peak throughput for
  transactional integrity with your business data. At volumes where that trade
  stops making sense, the right answer is a purpose-built broker.
- **Handlers run in-process.** There is no sandboxing or per-job resource
  limiting; a handler that blocks the event loop blocks its worker.
- **Completed jobs are never pruned.** A production deployment would need
  partitioning or a retention job.
