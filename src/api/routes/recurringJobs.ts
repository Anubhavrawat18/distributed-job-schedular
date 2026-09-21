import { Router, type Request, type Response, type NextFunction } from "express";
import { pool } from "../../db/client";
import { config } from "../../config";
import { nextOccurrence } from "../../scheduler/cron";
import type { RecurringJob } from "../../types/job";

const asyncRoute =
  (handler: (req: Request, res: Response) => Promise<unknown>) =>
  (req: Request, res: Response, next: NextFunction) => {
    handler(req, res).catch(next);
  };

export const recurringJobsRouter = Router();

recurringJobsRouter.post(
  "/recurring-jobs",

  asyncRoute(async (req, res) => {
    const { name, type, payload, cron, timezone, maxAttempts } = req.body ?? {};

    if (typeof name !== "string" || name.trim() === "") {
      return res.status(400).json({ error: "`name` must be a non-empty string" });
    }
    if (typeof type !== "string" || type.trim() === "") {
      return res.status(400).json({ error: "`type` must be a non-empty string" });
    }
    if (typeof cron !== "string" || cron.trim() === "") {
      return res.status(400).json({ error: "`cron` must be a non-empty string" });
    }
    if (
      payload !== undefined &&
      (typeof payload !== "object" || payload === null || Array.isArray(payload))
    ) {
      return res.status(400).json({ error: "`payload` must be a JSON object" });
    }
    if (
      maxAttempts !== undefined &&
      (!Number.isInteger(maxAttempts) || maxAttempts < 1)
    ) {
      return res.status(400).json({ error: "`maxAttempts` must be an integer >= 1" });
    }

    const tz = typeof timezone === "string" && timezone.trim() !== "" ? timezone : "UTC";

    // Validate the expression at creation time rather than letting the
    // scheduler discover it is unparseable at 3am. Also gives us the first
    // occurrence to store.
    let firstRunAt: Date;
    try {
      firstRunAt = nextOccurrence(cron.trim(), tz, new Date());
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return res.status(400).json({ error: `invalid cron expression: ${message}` });
    }

    try {
      const { rows } = await pool.query<RecurringJob>(
        `INSERT INTO recurring_jobs (name, type, payload, cron_expression, timezone, max_attempts, next_run_at)
         VALUES ($1, $2, $3, $4, $5, COALESCE($6::int, $7::int), $8)
         RETURNING *`,
        [
          name.trim(),
          type.trim(),
          payload ?? {},
          cron.trim(),
          tz,
          maxAttempts ?? null,
          config.retry.maxAttempts,
          firstRunAt,
        ],
      );
      return res.status(201).json(rows[0]);
    } catch (err) {
      // 23505 = unique_violation on recurring_jobs.name
      if ((err as { code?: string }).code === "23505") {
        return res
          .status(409)
          .json({ error: `a recurring job named "${name.trim()}" already exists` });
      }
      throw err;
    }
  }),
);

recurringJobsRouter.get(
  "/recurring-jobs",

  asyncRoute(async (_req, res) => {
    const { rows } = await pool.query<RecurringJob>(
      `SELECT * FROM recurring_jobs ORDER BY name`,
    );
    return res.json({ recurringJobs: rows });
  }),
);

// Disable rather than delete by default: deleting a definition sets
// recurring_job_id to NULL on every job it produced, losing the provenance of
// work that already ran.
recurringJobsRouter.patch(
  "/recurring-jobs/:id",

  asyncRoute(async (req, res) => {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: "`id` must be a positive integer" });
    }

    const { enabled } = req.body ?? {};
    if (typeof enabled !== "boolean") {
      return res.status(400).json({ error: "`enabled` must be a boolean" });
    }

    const { rows } = await pool.query<RecurringJob>(
      `UPDATE recurring_jobs SET enabled = $2, updated_at = now()
       WHERE id = $1 RETURNING *`,
      [id, enabled],
    );

    if (rows.length === 0) {
      return res.status(404).json({ error: "recurring job not found" });
    }
    return res.json(rows[0]);
  }),
);
