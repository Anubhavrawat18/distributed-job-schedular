import { readFileSync } from "fs";
import { join } from "path";
import { pool } from "./client";

// Applies schema.sql. Docker Compose runs it automatically on a fresh volume;
// this exists for an already-created database (or a local Postgres install).

// TASK:
// schema.sql ->read SQL command -> connectto POSTGRES SQL through any connection form the pool ->execute the command -> create or update the table or schema ->close database connection
async function main(): Promise<void> {
  const sql = readFileSync(join(__dirname, "schema.sql"), "utf8");
  await pool.query(sql);
  console.log("[db] schema applied");
  await pool.end();
}

main().catch((err) => {
  console.error("[db] schema init failed", err);
  process.exit(1);
});
