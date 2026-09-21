export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface Job {
    id: number;
    type: string;
    payload: Record<string, unknown>;
    status: JobStatus;
    result: Record<string, unknown> | null;
    error: string | null;
    worker_id: string | null;
    attempts: number;
    max_attempts: number;
    next_run_at: string;
    recurring_job_id: number | null;
    scheduled_for: string | null;
    created_at: string;
    updated_at: string;
}

export interface RecurringJob {
    id: number;
    name: string;
    type: string;
    payload: Record<string, unknown>;
    cron_expression: string;
    timezone: string;
    max_attempts: number;
    enabled: boolean;
    next_run_at: string;
    last_enqueued_at: string | null;
    created_at: string;
    updated_at: string;
}

export interface DeadLetterJob {
    id: number;
    job_id: number;
    attempts: number;
    final_error: string;
    died_at: string;
}

export interface JobExecution {
    id: number;
    job_id: number;
    worker_id: string;
    started_at: string;
}
