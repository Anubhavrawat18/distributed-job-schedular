# Project: Distributed Job Scheduler (from scratch)

## What this project is

A distributed job scheduler built from first principles — not a wrapper around
BullMQ/RabbitMQ/Airflow. The goal is to demonstrate deep understanding of
distributed systems fundamentals: durability, concurrency-safe job claiming,
retries/backoff, scheduling, idempotency, and observability.

Postgres is the source of truth (not Redis) — this is intentional, so that
locking and durability are solved explicitly rather than inherited for free.

## Tech stack

- **Language:** Node.js + TypeScript
- **Database:** PostgreSQL
- **API framework:** Fastify (or Express — pick one and stay consistent)
- **DB access:** raw `pg` driver preferred over an ORM, to keep SQL visible
  and explainable (this is a portfolio project — the SQL *is* the point)
- **Containerization:** Docker Compose (Postgres + N worker containers)
- **Dashboard (later phase):** minimal React app or server-rendered HTML

## Project structure

```
job-scheduler/
├── CLAUDE.md                 # this file
├── PROGRESS.md               # running build log (see "Progress log" below)
├── docker-compose.yml
├── package.json
├── tsconfig.json
├── src/
│   ├── db/
│   │   ├── schema.sql        # table definitions, run on init
│   │   ├── migrations/       # incremental schema changes per phase
│   │   └── client.ts         # pg pool/connection setup
│   ├── api/
│   │   ├── server.ts         # Fastify/Express bootstrap
│   │   └── routes/
│   │       └── jobs.ts       # enqueue / query job status endpoints
│   ├── worker/
│   │   ├── worker.ts         # worker process entrypoint
│   │   ├── claimJob.ts       # SELECT ... FOR UPDATE SKIP LOCKED logic
│   │   └── executeJob.ts     # job execution + result handling
│   ├── scheduler/
│   │   └── scheduler.ts      # cron/delayed job dispatch process
│   ├── retry/
│   │   └── backoff.ts        # exponential backoff + jitter logic
│   ├── types/
│   │   └── job.ts            # shared TypeScript types
│   └── config.ts
├── dashboard/                 # added in the observability phase
└── tests/
```

## Development workflow — READ THIS CAREFULLY

We build **phase by phase, in a loop**. Do not implement multiple phases in
one pass, and do not scaffold future phases ahead of time "for convenience."

The loop for every phase is:

1. Implement **only** the current phase's scope.
2. Update `PROGRESS.md` (see format below) describing what was built.
3. Stop and hand control back for review — do not proceed to the next phase
   or start unrelated cleanup without explicit go-ahead.
4. Once approved, make a **git commit** for that phase before moving on.

### Phases (in order)

1. Basic queue — schema, enqueue API, single worker polling loop
2. Concurrency safety — multiple workers, `SKIP LOCKED`, no double-claiming
3. Retries & dead-letter queue — exponential backoff, jitter, DLQ table
4. Scheduling — delayed jobs (`run_at`) and recurring jobs (cron expressions)
5. Priority & rate limiting — priority ordering, per-job-type concurrency caps
6. Idempotency — idempotency keys, safe re-execution semantics
7. Observability — dashboard showing queue state, history, failures
8. Graceful shutdown & crash recovery — `SIGTERM` handling, stuck-job leases

## Git conventions

- Commit at the end of each approved phase — not mid-phase, not batched
  across phases.
- Commit messages should be meaningful and describe *what* and *why*, e.g.:
  `feat(worker): add SKIP LOCKED job claiming to prevent double-processing`
  not `updates` or `phase 2`.
- **Do not add Claude/Anthropic as a commit author, co-author, or
  contributor.** No `Co-authored-by` trailers, no author field changes.
  Commits should reflect only the human developer.
- Don't push or set/change the remote — the user manages that.

## Progress log — `PROGRESS.md`

Maintain a running markdown log in `PROGRESS.md` at the project root,
updated after each phase. For every entry include:

- **What was built** — the concrete feature/component
- **Purpose** — what distributed-systems problem it solves
- **How it works** — the mechanism, briefly (e.g., "uses `FOR UPDATE SKIP
  LOCKED` so concurrent workers never claim the same row")

This file doubles as interview prep material, so write entries as if
explaining the decision to someone reviewing the project later — not just
a changelog.

Example entry format:

```markdown
## Phase 2 — Concurrency-safe job claiming

**What:** Workers now claim jobs via `SELECT ... FOR UPDATE SKIP LOCKED`
instead of a plain `SELECT` + `UPDATE`.

**Purpose:** Prevent two workers from picking up and executing the same
job simultaneously when running multiple worker processes.

**How:** Wrapped the claim query in a transaction using `FOR UPDATE SKIP
LOCKED`, which lets concurrent transactions skip rows already locked by
another transaction instead of blocking on them. Benchmarked against a
naive approach to confirm no duplicate execution under 5 concurrent workers.
```

## General rules

- Prefer clarity over cleverness — this code will be explained out loud.
- Don't introduce a library to solve a problem that's the point of the
  exercise (e.g., don't reach for a queue library instead of writing the
  polling/locking logic yourself). Libraries for incidental concerns (cron
  parsing, env config, testing) are fine.
- Keep phases isolated enough that each one is a clean, reviewable diff.
