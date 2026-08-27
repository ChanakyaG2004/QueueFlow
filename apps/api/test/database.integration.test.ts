import assert from "node:assert/strict";
import crypto from "node:crypto";
import test from "node:test";
import { Pool } from "pg";
import { createJob, QuotaExceededError } from "../src/job-service.js";

const databaseUrl = process.env.TEST_DATABASE_URL;

test("PostgreSQL atomically persists job, event, and outbox and enforces quota", { skip: !databaseUrl }, async () => {
  const pool = new Pool({ connectionString: databaseUrl });
  const tenantId = crypto.randomUUID();
  try {
    await pool.query(
      "INSERT INTO tenants (id, name, api_key, max_active_jobs) VALUES ($1, 'Integration', $2, 1)",
      [tenantId, `integration-${tenantId}`],
    );
    const job = await createJob(pool, tenantId, {
      type: "text_analysis", text: "integration", cpu: 1, memoryMb: 256, gpu: 0, priority: 4,
    });
    const persisted = await pool.query(
      `SELECT j.id,
              (SELECT COUNT(*)::int FROM job_events e WHERE e.job_id = j.id) AS event_count,
              (SELECT COUNT(*)::int FROM job_outbox o WHERE o.job_id = j.id) AS outbox_count
       FROM jobs j WHERE j.id = $1 AND j.tenant_id = $2`,
      [job.id, tenantId],
    );
    assert.equal(persisted.rowCount, 1);
    assert.equal(persisted.rows[0].event_count, 1);
    assert.equal(persisted.rows[0].outbox_count, 1);
    await assert.rejects(
      createJob(pool, tenantId, { type: "simulated_compute", cpu: 1, memoryMb: 256, gpu: 0, priority: 0 }),
      QuotaExceededError,
    );
  } finally {
    await pool.query("DELETE FROM jobs WHERE tenant_id = $1", [tenantId]);
    await pool.query("DELETE FROM tenants WHERE id = $1", [tenantId]);
    await pool.end();
  }
});
