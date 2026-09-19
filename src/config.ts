import "dotenv/config";

export const config = {
    port: Number(process.env.PORT ?? 3000),
    databaseUrl:
        process.env.DATABASE_URL ??
        "postgres://scheduler:scheduler@localhost:5432/job_scheduler",
    worker: {
        pollIntervalMs: Number(process.env.WORKER_POLL_INTERVAL_MS ?? 1000),
    },
};
