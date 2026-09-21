import {
  Router,
  type Request,
  type Response,
  type NextFunction,
} from "express";
import { pool } from "../../db/client";
import { config } from "../../config";
import type { Job } from "../../types/job";

// Express 4 does not forward rejected promises to the error handler, so async
// handlers are wrapped to make DB failures return a 500 instead of hanging.

// a wrapper for Express route handlers that automatically catches errors from async functions and passes them to Express's error-handling middleware....you pass the function to run, the middleware
// it tries calling a function and then calls next(error) ifPromise rejected
const asyncRoute =
  // below are the attributes to be passed to a function...i.e a function has to be passed in the asyncRoute...in our case we are passing a callback function (arrow fn)
  (handler: (req: Request, res: Response) => Promise<unknown>) =>
    (req: Request, res: Response, next: NextFunction) => {
      handler(req, res).catch(next);
    };

export const jobsRouter = Router();

// Enqueue a job. Durability comes from the INSERT itself: once this returns,
// the job survives an API crash because Postgres — not an in-memory queue —
// is holding it.

jobsRouter.post(
  "/jobs",

  asyncRoute(async (req, res) => {
    // Get `type` and `payload` from the request body.
    // If req.body doesn't exist, use an empty object instead.
    const { type, payload, maxAttempts, runAt, delaySeconds } = req.body ?? {};

    // Make sure `type` is a string and is not empty.
    if (typeof type !== "string" || type.trim() === "") {
      // If validation fails, return HTTP 400 (Bad Request).
      return res
        .status(400)
        .json({ error: "`type` must be a non-empty string" });
    }

    // If `payload` was provided, make sure it is a JSON object.
    // It cannot be null and it cannot be an array.
    if (
      payload !== undefined &&
      (typeof payload !== "object" ||
        payload === null ||
        Array.isArray(payload))
    ) {
      // If validation fails, return HTTP 400.
      return res.status(400).json({ error: "`payload` must be a JSON object" });
    }

    // Retry budget is per job: a cheap idempotent job can afford many attempts,
    // a payment call should get very few.
    if (
      maxAttempts !== undefined &&
      (!Number.isInteger(maxAttempts) || maxAttempts < 1)
    ) {
      return res
        .status(400)
        .json({ error: "`maxAttempts` must be an integer >= 1" });
    }

    // A delayed job needs no special machinery: next_run_at already gates
    // eligibility for retry backoff, so "run this later" is the same mechanism
    // with a different starting value.
    if (runAt !== undefined && delaySeconds !== undefined) {
      return res
        .status(400)
        .json({ error: "provide `runAt` or `delaySeconds`, not both" });
    }

    let nextRunAt: Date | null = null;

    if (runAt !== undefined) {
      const parsed = new Date(runAt);
      if (Number.isNaN(parsed.getTime())) {
        return res
          .status(400)
          .json({ error: "`runAt` must be a valid ISO 8601 timestamp" });
      }
      nextRunAt = parsed;
    }

    if (delaySeconds !== undefined) {
      if (typeof delaySeconds !== "number" || !Number.isFinite(delaySeconds) || delaySeconds < 0) {
        return res
          .status(400)
          .json({ error: "`delaySeconds` must be a non-negative number" });
      }
      nextRunAt = new Date(Date.now() + delaySeconds * 1000);
    }

    // Insert the new job into the database.
    // $1..$5 are placeholders for the values in the array below.
    //
    // RETURNING * tells PostgreSQL to return the newly inserted row.
    const { rows } = await pool.query<Job>(
      `INSERT INTO jobs (type, payload, max_attempts, next_run_at)
             VALUES ($1, $2, COALESCE($3::int, $4::int), COALESCE($5::timestamptz, now()))
             RETURNING *`,
      [
        type.trim(),
        payload ?? {},
        maxAttempts ?? null,
        config.retry.maxAttempts,
        nextRunAt,
      ],
    );

    // Return the newly created job with HTTP 201 (Created).
    return res.status(201).json(rows[0]);
  }),
);

// The dead-letter queue: jobs that exhausted their retry budget, newest first.
// Registered before /jobs/:id so "dead-letter" is not parsed as an id.
jobsRouter.get(
  "/jobs/dead-letter",

  asyncRoute(async (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 50, 200);

    const { rows } = await pool.query(
      `SELECT d.job_id, d.attempts, d.final_error, d.died_at,
              j.type, j.payload
       FROM dead_letter_jobs d
       JOIN jobs j ON j.id = d.job_id
       ORDER BY d.died_at DESC
       LIMIT $1`,
      [limit],
    );

    return res.json({ deadLetterJobs: rows });
  }),
);

jobsRouter.get(
  "/jobs/:id",

  asyncRoute(async (req, res) => {
    // Get the `id` from the URL.
    // Example: /jobs/123 → req.params.id is "123"
    const id = Number(req.params.id);

    // Make sure the ID is a positive integer.
    if (!Number.isInteger(id) || id <= 0) {
      // If invalid, return HTTP 400.
      return res.status(400).json({ error: "`id` must be a positive integer" });
    }

    // Find the job with the given ID.
    const { rows } = await pool.query<Job>(`SELECT * FROM jobs WHERE id = $1`, [
      id,
    ]);

    // If no job was found, return HTTP 404 (Not Found).
    if (rows.length === 0) {
      return res.status(404).json({ error: "job not found" });
    }

    // Return the first (and only) matching job.
    return res.json(rows[0]);
  }),
);

jobsRouter.get(
  "/jobs",

  asyncRoute(async (req, res) => {
    // Get the `status` query parameter.
    // Example: /jobs?status=pending
    //
    // If status isn't a string, use null.
    const status =
      typeof req.query.status === "string" ? req.query.status : null;

    // Get the `limit` query parameter.
    // If no valid limit is provided, use 50.
    // Never allow the limit to be greater than 200 -> done to prevent someone from requesting an unnecessarily large number of jobs at once
    const limit = Math.min(Number(req.query.limit) || 50, 200);

    // Get jobs from the database.
    //
    // $1 → status
    // $2 → limit
    //
    // If status is null, the condition
    // `($1::text IS NULL OR status = $1)` allows all jobs.
    //
    // Otherwise, only jobs matching that status are returned.
    //
    // ORDER BY created_at DESC → newest jobs first.
    // LIMIT $2 → return at most `limit` jobs.
    const { rows } = await pool.query<Job>(
      `SELECT * FROM jobs
             WHERE ($1::text IS NULL OR status = $1)
             ORDER BY created_at DESC
             LIMIT $2`,
      [status, limit],
    );

    // Return the jobs as an object.
    return res.json({ jobs: rows });
  }),
);
