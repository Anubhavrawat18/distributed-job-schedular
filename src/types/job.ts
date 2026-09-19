export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface Job {
    id: number;
    type: string;
    payload: Record<string, unknown>;
    status: JobStatus;
    result: Record<string, unknown> | null;
    error: string | null;
    created_at: string;
    updated_at: string;
}
