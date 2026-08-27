import assert from "node:assert/strict";
import test from "node:test";
import type { Pool, PoolClient } from "pg";
import { createJob, QuotaExceededError } from "../src/job-service.js";

function fakePool(activeJobs = 0, maximum = 10) {
  const statements: string[] = [];
  const client = {
    async query(sql: string) {
      statements.push(sql.trim());
      if (sql.includes("SELECT max_active_jobs")) return { rowCount: 1, rows: [{ max_active_jobs: maximum }] };
      if (sql.includes("COUNT(*)")) return { rowCount: 1, rows: [{ active_jobs: activeJobs }] };
      if (sql.includes("INSERT INTO jobs")) return { rowCount: 1, rows: [{ id: "job-1", priority: 5 }] };
      return { rowCount: 1, rows: [] };
    },
    release() {},
  } as unknown as PoolClient;
  return {
    pool: { connect: async () => client } as unknown as Pool,
    statements,
  };
}

test("successful creation atomically writes the job, lifecycle event, and outbox record", async () => {
  const { pool, statements } = fakePool();
  const job = await createJob(pool, "tenant-1", {
    type: "text_analysis", text: "hello", cpu: 1, memoryMb: 256, gpu: 0, priority: 5,
  });
  assert.equal(job.id, "job-1");
  assert.ok(statements.some((sql) => sql.includes("INSERT INTO jobs")));
  assert.ok(statements.some((sql) => sql.includes("INSERT INTO job_events")));
  assert.ok(statements.some((sql) => sql.includes("INSERT INTO job_outbox")));
  assert.equal(statements.at(-1), "COMMIT");
});

test("quota enforcement rolls back before creating a job", async () => {
  const { pool, statements } = fakePool(10, 10);
  await assert.rejects(
    createJob(pool, "tenant-1", { type: "simulated_compute", cpu: 1, memoryMb: 256, gpu: 0, priority: 0 }),
    QuotaExceededError,
  );
  assert.equal(statements.at(-1), "ROLLBACK");
  assert.equal(statements.some((sql) => sql.includes("INSERT INTO jobs")), false);
});
