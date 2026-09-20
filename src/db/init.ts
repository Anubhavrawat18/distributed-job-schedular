import { readFileSync, readdirSync, existsSync } from "fs";
import { join } from "path";
import { pool } from "./client";

// Applies schema.sql, then every file in migrations/ in filename order.
// Both are written with IF NOT EXISTS, so re-running is a no-op.

// TASK:
// schema.sql ->read SQL command -> connectto POSTGRES SQL through any connection form the pool ->execute the command -> create or update the table or schema ->close database connection
async function main(): Promise<void> {
  const sql = readFileSync(join(__dirname, "schema.sql"), "utf8");
  await pool.query(sql);
  console.log("[db] schema applied");

  const migrationsDir = join(__dirname, "migrations");
  if (existsSync(migrationsDir)) {
    const files = readdirSync(migrationsDir)
      .filter((f) => f.endsWith(".sql"))
      .sort();

    for (const file of files) {
      await pool.query(readFileSync(join(migrationsDir, file), "utf8"));
      console.log(`[db] migration applied: ${file}`);
    }
  }

  await pool.end();
}

main().catch((err) => {
  console.error("[db] schema init failed", err);
  process.exit(1);
});
