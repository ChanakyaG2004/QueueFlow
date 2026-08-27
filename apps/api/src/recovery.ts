import type { Pool } from "pg";
import { logger } from "./logger.js";
import { incrementMetric } from "./metrics.js";

export class WorkerRecovery {
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly pool: Pool,
    private readonly staleAfterSeconds = 120,
    private readonly intervalMs = 30_000,
  ) {}

  start() {
    if (this.timer) return;
    this.timer = setInterval(() => void this.tick(), this.intervalMs);
    this.timer.unref();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }

  async tick() {
    if (this.running) return 0;
    this.running = true;
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const staleWorkers = await client.query<{ id: string }>(
        `SELECT id FROM workers
         WHERE status <> 'OFFLINE'
           AND last_heartbeat < NOW() - ($1 * INTERVAL '1 second')
         FOR UPDATE SKIP LOCKED`,
        [this.staleAfterSeconds],
      );

      let recovered = 0;
      for (const worker of staleWorkers.rows) {
        await client.query("UPDATE workers SET status = 'OFFLINE', current_job_id = NULL WHERE id = $1", [worker.id]);
        const jobs = await client.query<{ id: string; attempt_count: number; max_attempts: number }>(
          `UPDATE jobs
           SET status = 'RETRYING', worker_id = NULL,
               error = 'Assigned worker heartbeat expired; awaiting SQS redelivery'
           WHERE worker_id = $1 AND status = 'RUNNING'
           RETURNING id, attempt_count, max_attempts`,
          [worker.id],
        );

        for (const job of jobs.rows) {
          await client.query(
            `INSERT INTO job_events (job_id, event_type, message, metadata)
             VALUES ($1, 'RECOVERED', $2, $3)`,
            [
              job.id,
              "Stale worker detected; job made retryable",
              JSON.stringify({
                worker_id: worker.id,
                attempt: job.attempt_count,
                max_attempts: job.max_attempts,
                stale_after_seconds: this.staleAfterSeconds,
              }),
            ],
          );
          recovered += 1;
          logger.warn("job recovered from stale worker", { job_id: job.id, worker_id: worker.id });
        }
        logger.warn("worker marked offline", { worker_id: worker.id, recovered_jobs: jobs.rowCount ?? 0 });
      }

      await client.query("COMMIT");
      if (recovered) incrementMetric("worker_recoveries", recovered);
      return recovered;
    } catch (error) {
      await client.query("ROLLBACK");
      logger.error("worker recovery scan failed", { error: error instanceof Error ? error.message : String(error) });
      return 0;
    } finally {
      client.release();
      this.running = false;
    }
  }
}
