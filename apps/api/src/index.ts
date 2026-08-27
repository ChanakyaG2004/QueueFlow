import dotenv from "dotenv";
import { createApp } from "./app.js";
import { pool } from "./db.js";
import { logger } from "./logger.js";
import { runMigrations } from "./migrate.js";
import { OutboxPublisher } from "./outbox.js";
import { WorkerRecovery } from "./recovery.js";
import { sendJobToQueue } from "./sqs.js";

dotenv.config();

const port = Number(process.env.PORT ?? 3001);

async function main() {
  await runMigrations();
  const publisher = new OutboxPublisher(pool, sendJobToQueue, {
    pollIntervalMs: Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 1_000),
    lockTimeoutSeconds: Number(process.env.OUTBOX_LOCK_TIMEOUT_SECONDS ?? 30),
    workerHealthySeconds: Number(process.env.WORKER_STALE_AFTER_SECONDS ?? 120),
    priorityAgingSeconds: Number(process.env.PRIORITY_AGING_SECONDS ?? 60),
    retryBaseMs: Number(process.env.OUTBOX_RETRY_BASE_MS ?? 1_000),
    retryMaxMs: Number(process.env.OUTBOX_RETRY_MAX_MS ?? 60_000),
  });
  const recovery = new WorkerRecovery(
    pool,
    Number(process.env.WORKER_STALE_AFTER_SECONDS ?? 120),
    Number(process.env.WORKER_RECOVERY_INTERVAL_MS ?? 30_000),
  );

  publisher.start();
  recovery.start();
  const server = createApp(pool).listen(port, () => logger.info("QueueFlow API started", { port }));

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info("API shutdown started", { signal });
    recovery.stop();
    server.close();
    await publisher.stop();
    await pool.end();
    logger.info("API shutdown complete", { signal });
  };

  process.on("SIGTERM", () => void shutdown("SIGTERM"));
  process.on("SIGINT", () => void shutdown("SIGINT"));
}

main().catch(async (error) => {
  logger.error("API startup failed", { error: error instanceof Error ? error.message : String(error) });
  await pool.end().catch(() => undefined);
  process.exitCode = 1;
});
