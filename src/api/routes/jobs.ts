import {
  Router,
  type Request,
  type Response,
  type NextFunction,
} from "express";
import { pool } from "../../db/client";
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
    const { type, payload } = req.body ?? {};

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

    // Insert the new job into the database.
    // $1 and $2 are placeholders for the values in the array below.
    //
    // RETURNING * tells PostgreSQL to return the newly inserted row.
    const { rows } = await pool.query<Job>(
      `INSERT INTO jobs (type, payload)
             VALUES ($1, $2)
             RETURNING *`,
      [type.trim(), payload ?? {}],
    );

    // Return the newly created job with HTTP 201 (Created).
    return res.status(201).json(rows[0]);
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
