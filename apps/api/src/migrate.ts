import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pool } from "./db.js";
import { logger } from "./logger.js";

export async function runMigrations() {
  const migrationsDirectory = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../migrations");
  const files = (await fs.readdir(migrationsDirectory)).filter((file) => file.endsWith(".sql")).sort();
  const client = await pool.connect();

  try {
    await client.query("SELECT pg_advisory_lock(716328104)");
    await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`);

    for (const file of files) {
      const existing = await client.query("SELECT 1 FROM schema_migrations WHERE name = $1", [file]);
      if (existing.rowCount) continue;

      const sql = await fs.readFile(path.join(migrationsDirectory, file), "utf8");
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query("INSERT INTO schema_migrations (name) VALUES ($1)", [file]);
        await client.query("COMMIT");
        logger.info("database migration applied", { migration: file });
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }

    if (process.env.SEED_DEMO_TENANT === "true") {
      await client.query(
        `INSERT INTO tenants (id, name, api_key, max_active_jobs)
         VALUES (gen_random_uuid(), 'QueueFlow Demo', 'queueflow-demo-key', 10)
         ON CONFLICT (api_key) DO NOTHING`,
      );
      logger.warn("development demo tenant is enabled");
    }
  } finally {
    await client.query("SELECT pg_advisory_unlock(716328104)").catch(() => undefined);
    client.release();
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runMigrations()
    .then(() => pool.end())
    .catch((error) => {
      logger.error("database migration failed", { error: error instanceof Error ? error.message : String(error) });
      process.exitCode = 1;
    });
}
