import express, { type ErrorRequestHandler } from "express";
import { config } from "../config";
import { pool } from "../db/client";
import { jobsRouter } from "./routes/jobs";
import { recurringJobsRouter } from "./routes/recurringJobs";

const app = express();

app.use(express.json());

// _req -> a naming convention for when you know a variable is there but not used
// here in the function we do not use req but still give it a place in function. Thus, we start with _
app.get("/health", async (_req, res) => {
  // try to fetch a record
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch {
    //   if failed to get a row means connections does not exist
    res.status(503).json({ ok: false, error: "database unreachable" });
  }
});

app.use(jobsRouter);
app.use(recurringJobsRouter);

const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
  console.error("[api] unhandled error", err);
  res.status(500).json({ error: "internal server error" });
};
app.use(errorHandler);

app.listen(config.port, () => {
  console.log(`[api] listening on http://localhost:${config.port}`);
});
