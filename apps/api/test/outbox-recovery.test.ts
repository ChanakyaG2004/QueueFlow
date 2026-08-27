import assert from "node:assert/strict";
import test from "node:test";
import type { Pool } from "pg";
import { OutboxPublisher } from "../src/outbox.js";
import { WorkerRecovery } from "../src/recovery.js";

test("an SQS failure leaves an outbox row unpublished and schedules a retry", async () => {
  const updates: { sql: string; values?: unknown[] }[] = [];
  const pool = { query: async (sql: string, values?: unknown[]) => {
    updates.push({ sql, values });
    return { rowCount: 1, rows: [] };
  } } as unknown as Pool;
  const publisher = new OutboxPublisher(pool, async () => { throw new Error("SQS unavailable"); }, { retryBaseMs: 1 });
  (publisher as unknown as { claimNext: () => Promise<unknown> }).claimNext = async () => ({
    id: "outbox-1", job_id: "job-1", payload: { jobId: "job-1", type: "simulated_compute" }, attempts: 1,
  });
  assert.equal(await publisher.tick(), false);
  assert.ok(updates[0].sql.includes("last_error"));
  assert.equal(updates[0].values?.[2], "SQS unavailable");
});

test("stale worker recovery marks jobs retryable and records an event atomically", async () => {
  const statements: string[] = [];
  const client = {
    async query(sql: string) {
      statements.push(sql.trim());
      if (sql.includes("SELECT id FROM workers")) return { rows: [{ id: "worker-1" }], rowCount: 1 };
      if (sql.includes("UPDATE jobs")) return { rows: [{ id: "job-1", attempt_count: 1, max_attempts: 3 }], rowCount: 1 };
      return { rows: [], rowCount: 1 };
    },
    release() {},
  };
  const pool = { connect: async () => client } as unknown as Pool;
  assert.equal(await new WorkerRecovery(pool).tick(), 1);
  assert.ok(statements.some((sql) => sql.includes("status = 'OFFLINE'")));
  assert.ok(statements.some((sql) => sql.includes("status = 'RETRYING'")));
  assert.ok(statements.some((sql) => sql.includes("'RECOVERED'")));
  assert.equal(statements.at(-1), "COMMIT");
});
