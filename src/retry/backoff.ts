export interface BackoffOptions {
  baseMs: number;
  maxMs: number;
}

/**
 * Exponential backoff with equal jitter.
 *
 * Exponential growth is the easy half: wait longer after each failure so a
 * struggling dependency gets room to recover instead of being hammered.
 *
 * Jitter is the half people skip, and it is what actually prevents the outage
 * from repeating. Without it, retry delays are deterministic: if a downstream
 * API goes down and 1000 jobs fail within the same second, all 1000 compute the
 * same delay, wake at the same instant, and stampede the service the moment it
 * comes back — re-triggering the outage they were retrying because of. This is
 * the thundering herd, and the fix is to smear the retries across a window
 * rather than aligning them on one.
 *
 * Equal jitter — half the delay fixed, half random — is the choice here over
 * AWS's "full jitter" (`random(0, exponential)`). Full jitter spreads load
 * slightly better, but because its lower bound is zero it can schedule a retry
 * almost immediately, which defeats the point of backing off at all for the
 * unlucky job. Equal jitter guarantees a floor of `exponential / 2` while still
 * breaking up the herd, so the delay is both genuinely increasing and spread.
 */
export function computeBackoffMs(attempt: number, opts: BackoffOptions): number {
  const exponential = Math.min(opts.maxMs, opts.baseMs * 2 ** (attempt - 1));
  const half = exponential / 2;
  return Math.round(half + Math.random() * half);
}
