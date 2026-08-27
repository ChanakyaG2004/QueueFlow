import crypto from "node:crypto";
import type { Pool } from "pg";
import type { CreateJobInput } from "./validation.js";

export class QuotaExceededError extends Error {}

export async function createJob(pool: Pool, tenantId: string, input: CreateJobInput) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const tenantResult = await client.query(
      "SELECT max_active_jobs FROM tenants WHERE id = $1 FOR UPDATE",
      [tenantId],
    );
    if (!tenantResult.rowCount) throw new Error("Authenticated tenant no longer exists");

    const activeResult = await client.query(
      `SELECT COUNT(*)::int AS active_jobs
       FROM jobs
       WHERE tenant_id = $1 AND status IN ('QUEUED', 'RUNNING', 'RETRYING')`,
      [tenantId],
    );

    if (activeResult.rows[0].active_jobs >= tenantResult.rows[0].max_active_jobs) {
      throw new QuotaExceededError("Active job quota exceeded");
    }

    const id = crypto.randomUUID();
    const jobResult = await client.query(
      `INSERT INTO jobs (
         id, tenant_id, type, status, cpu_required,
         memory_required_mb, gpu_required, priority
       ) VALUES ($1, $2, $3, 'QUEUED', $4, $5, $6, $7)
       RETURNING *`,
      [id, tenantId, input.type, input.cpu, input.memoryMb, input.gpu, input.priority],
    );

    await client.query(
      `INSERT INTO job_events (job_id, event_type, message, metadata)
       VALUES ($1, 'SUBMITTED', $2, $3)`,
      [
        id,
        "Job submitted to QueueFlow",
        JSON.stringify({
          cpu: input.cpu,
          memoryMb: input.memoryMb,
          gpu: input.gpu,
          priority: input.priority,
        }),
      ],
    );

    await client.query(
      `INSERT INTO job_outbox (job_id, payload)
       VALUES ($1, $2)`,
      [id, JSON.stringify({ jobId: id, type: input.type, text: input.text })],
    );

    await client.query("COMMIT");
    return jobResult.rows[0];
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
