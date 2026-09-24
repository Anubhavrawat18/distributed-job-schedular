import { Router, type Request, type Response, type NextFunction } from "express";
import { pool } from "../../db/client";
import type { JobTypeLimit } from "../../types/job";

const asyncRoute =
  (handler: (req: Request, res: Response) => Promise<unknown>) =>
  (req: Request, res: Response, next: NextFunction) => {
    handler(req, res).catch(next);
  };

export const jobTypeLimitsRouter = Router();

// Current caps, alongside how many of each type are running right now — the
// number you actually want when deciding whether a cap is set too low.
jobTypeLimitsRouter.get(
  "/job-type-limits",

  asyncRoute(async (_req, res) => {
    const { rows } = await pool.query(
      `SELECT l.type, l.max_concurrency, l.updated_at,
              (SELECT count(*) FROM jobs j WHERE j.status = 'running' AND j.type = l.type) AS running
       FROM job_type_limits l
       ORDER BY l.type`,
    );
    return res.json({ limits: rows });
  }),
);

// Upsert: setting a cap for a type that already has one replaces it.
jobTypeLimitsRouter.put(
  "/job-type-limits/:type",

  asyncRoute(async (req, res) => {
    const type = req.params.type;
    const { maxConcurrency } = req.body ?? {};

    if (!Number.isInteger(maxConcurrency) || maxConcurrency < 1) {
      return res
        .status(400)
        .json({ error: "`maxConcurrency` must be an integer >= 1" });
    }

    const { rows } = await pool.query<JobTypeLimit>(
      `INSERT INTO job_type_limits (type, max_concurrency)
       VALUES ($1, $2)
       ON CONFLICT (type) DO UPDATE
         SET max_concurrency = EXCLUDED.max_concurrency, updated_at = now()
       RETURNING *`,
      [type, maxConcurrency],
    );

    return res.json(rows[0]);
  }),
);

jobTypeLimitsRouter.delete(
  "/job-type-limits/:type",

  asyncRoute(async (req, res) => {
    const { rowCount } = await pool.query(
      `DELETE FROM job_type_limits WHERE type = $1`,
      [req.params.type],
    );

    if (rowCount === 0) {
      return res.status(404).json({ error: "no limit set for that job type" });
    }
    return res.status(204).send();
  }),
);
