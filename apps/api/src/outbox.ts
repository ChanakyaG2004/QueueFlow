import crypto from "node:crypto";
import type { Pool } from "pg";
import { logger } from "./logger.js";
import { incrementMetric } from "./metrics.js";
import type { JobMessage, QueueSender } from "./sqs.js";

type OutboxRow = {
  id: string;
  job_id: string;
  payload: JobMessage | string;
  attempts: number;
};

type OutboxOptions = {
  pollIntervalMs?: number;
  lockTimeoutSeconds?: number;
  workerHealthySeconds?: number;
  priorityAgingSeconds?: number;
  retryBaseMs?: number;
  retryMaxMs?: number;
};

export class OutboxPublisher {
  private readonly publisherId = crypto.randomUUID();
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(
    private readonly pool: Pool,
    private readonly send: QueueSender,
    private readonly options: OutboxOptions = {},
  ) {}

  start() {
    if (this.timer) return;
    const interval = this.options.pollIntervalMs ?? 1_000;
    this.timer = setInterval(() => void this.tick(), interval);
    this.timer.unref();
    void this.tick();
  }

  async stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    while (this.running) await new Promise((resolve) => setTimeout(resolve, 10));
  }

  async tick() {
    if (this.running) return false;
    this.running = true;
    try {
      const row = await this.claimNext();
      if (!row) return false;

      const payload = typeof row.payload === "string" ? JSON.parse(row.payload) as JobMessage : row.payload;
      try {
        await this.send(payload);
        await this.pool.query(
          `UPDATE job_outbox
           SET published_at = NOW(), locked_at = NULL, locked_by = NULL, last_error = NULL
           WHERE id = $1 AND locked_by = $2`,
          [row.id, this.publisherId],
        );
        logger.info("outbox message published", { outbox_id: row.id, job_id: row.job_id, attempt: row.attempts });
        return true;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const base = this.options.retryBaseMs ?? 1_000;
        const maximum = this.options.retryMaxMs ?? 60_000;
        const delay = Math.min(maximum, base * 2 ** Math.min(row.attempts - 1, 10));
        const jitteredDelay = Math.round(delay * (0.8 + Math.random() * 0.4));
        await this.pool.query(
          `UPDATE job_outbox
           SET locked_at = NULL, locked_by = NULL, last_error = $3,
               next_attempt_at = NOW() + ($4 * INTERVAL '1 millisecond')
           WHERE id = $1 AND locked_by = $2`,
          [row.id, this.publisherId, message.slice(0, 2_000), jitteredDelay],
        );
        incrementMetric("outbox_publish_failures");
        logger.error("outbox publish failed", {
          outbox_id: row.id,
          job_id: row.job_id,
          attempt: row.attempts,
          retry_delay_ms: jitteredDelay,
          error: message,
        });
        return false;
      }
    } catch (error) {
      incrementMetric("outbox_scan_failures");
      logger.error("outbox scan failed", { error: error instanceof Error ? error.message : String(error) });
      return false;
    } finally {
      this.running = false;
    }
  }

  private async claimNext(): Promise<OutboxRow | undefined> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<OutboxRow>(
        `WITH candidate AS (
           SELECT o.id
           FROM job_outbox o
           JOIN jobs j ON j.id = o.job_id
           WHERE o.published_at IS NULL
             AND o.next_attempt_at <= NOW()
             AND (o.locked_at IS NULL OR o.locked_at < NOW() - ($1 * INTERVAL '1 second'))
             AND j.status = 'QUEUED'
             AND EXISTS (
               SELECT 1 FROM workers w
               WHERE w.last_heartbeat > NOW() - ($2 * INTERVAL '1 second')
                 AND w.status <> 'OFFLINE'
                 AND w.cpu_capacity >= j.cpu_required
                 AND w.memory_capacity_mb >= j.memory_required_mb
                 AND w.gpu_capacity >= j.gpu_required
             )
           ORDER BY (
             j.priority + LEAST(10, FLOOR(EXTRACT(EPOCH FROM (NOW() - j.created_at)) / $3))
           ) DESC, j.created_at ASC
           FOR UPDATE OF o SKIP LOCKED
           LIMIT 1
         )
         UPDATE job_outbox o
         SET locked_at = NOW(), locked_by = $4, attempts = attempts + 1
         FROM candidate
         WHERE o.id = candidate.id
         RETURNING o.id, o.job_id, o.payload, o.attempts`,
        [
          this.options.lockTimeoutSeconds ?? 30,
          this.options.workerHealthySeconds ?? 90,
          this.options.priorityAgingSeconds ?? 60,
          this.publisherId,
        ],
      );
      await client.query("COMMIT");
      return result.rows[0];
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
