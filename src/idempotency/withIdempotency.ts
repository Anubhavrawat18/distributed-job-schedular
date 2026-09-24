import type { PoolClient } from "pg";
import { pool } from "../db/client";
import { config } from "../config";

export interface IdempotentOutcome<T> {
  result: T;
  /** false when a previous execution had already performed this effect. */
  executed: boolean;
}

/**
 * Runs `fn` at most once for a given `key`, ever.
 *
 * The guarantee comes from committing the effect and the record of the effect
 * in a single transaction. Doing them separately cannot work in either order:
 *
 *   record first, then effect  → a crash between them loses the effect forever,
 *                                because the record now claims it happened
 *   effect first, then record  → a crash between them repeats the effect on the
 *                                next attempt, which is the original bug
 *
 * Only one transaction makes the pair atomic, which is why `fn` receives the
 * transaction's client and must do its writes through it. A side effect issued
 * on a different connection — or against a different system — is outside this
 * transaction and gets none of this protection.
 *
 * Concurrency is handled by Postgres rather than by us. Two workers racing on
 * the same key both attempt the INSERT; the second blocks on the first's
 * uncommitted row, and when the first commits, `ON CONFLICT DO NOTHING` returns
 * no row so the second takes the already-done path. If the first rolls back,
 * the second's insert succeeds and it performs the effect. Either way the
 * effect happens exactly once.
 */
export async function withIdempotency<T>(
  key: string,
  jobId: number | null,
  fn: (client: PoolClient) => Promise<T>,
): Promise<IdempotentOutcome<T>> {
  // Escape hatch so the duplicate-effect bug stays reproducible, in the same
  // spirit as CLAIM_STRATEGY=naive and CAP_ENFORCEMENT=approximate.
  if (config.idempotency.mode === "off") {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await fn(client);
      await client.query("COMMIT");
      return { result, executed: true };
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const { rows: claimed } = await client.query<{ key: string }>(
      `INSERT INTO idempotency_records (key, job_id)
       VALUES ($1, $2)
       ON CONFLICT (key) DO NOTHING
       RETURNING key`,
      [key, jobId],
    );

    if (claimed.length === 0) {
      // Someone already did this. Return what they got rather than redoing it.
      const { rows: existing } = await client.query<{ result: T | null }>(
        `SELECT result FROM idempotency_records WHERE key = $1`,
        [key],
      );
      await client.query("COMMIT");
      return { result: existing[0]?.result as T, executed: false };
    }

    const result = await fn(client);

    await client.query(
      `UPDATE idempotency_records SET result = $2 WHERE key = $1`,
      [key, result === undefined ? null : JSON.stringify(result)],
    );

    await client.query("COMMIT");
    return { result, executed: true };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
