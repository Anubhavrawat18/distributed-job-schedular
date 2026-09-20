export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface Job {
    id: number;
    type: string;
    payload: Record<string, unknown>;
    status: JobStatus;
    result: Record<string, unknown> | null;
    error: string | null;
    worker_id: string | null;
    created_at: string;
    updated_at: string;
}

export interface JobExecution {
    id: number;
    job_id: number;
    worker_id: string;
    started_at: string;
}
