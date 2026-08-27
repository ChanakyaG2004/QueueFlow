import assert from "node:assert/strict";
import test from "node:test";
import type { Pool } from "pg";
import request from "supertest";
import { createApp } from "../src/app.js";

function poolWithQuery(query: (sql: string, values?: unknown[]) => unknown) {
  return { query } as unknown as Pool;
}

test("protected endpoints reject missing and invalid API keys", async () => {
  const pool = poolWithQuery(() => ({ rowCount: 0, rows: [] }));
  const app = createApp(pool);
  assert.equal((await request(app).get("/jobs")).status, 401);
  assert.equal((await request(app).get("/jobs").set("x-api-key", "bad-key")).status, 401);
});

test("job listing always scopes the database query to the authenticated tenant", async () => {
  const calls: unknown[][] = [];
  const pool = poolWithQuery((sql, values = []) => {
    calls.push([sql, values]);
    if (sql.includes("FROM tenants")) return { rowCount: 1, rows: [{ id: "tenant-a" }] };
    return { rowCount: 1, rows: [{ id: "job-a", tenant_id: "tenant-a" }] };
  });
  const response = await request(createApp(pool)).get("/jobs").set("x-api-key", "key-a");
  assert.equal(response.status, 200);
  assert.deepEqual(calls.at(-1)?.[1], ["tenant-a"]);
});

test("POST /jobs returns useful 400 responses for invalid and malformed payloads", async () => {
  const pool = poolWithQuery(() => ({ rowCount: 1, rows: [{ id: "tenant-a" }] }));
  const app = createApp(pool);
  const invalid = await request(app).post("/jobs").set("x-api-key", "key-a").send({ type: "text_analysis" });
  assert.equal(invalid.status, 400);
  assert.equal(invalid.body.error, "Invalid job payload");
  assert.equal(invalid.body.details[0].path, "text");

  const malformed = await request(app)
    .post("/jobs")
    .set("x-api-key", "key-a")
    .set("content-type", "application/json")
    .send('{"type":');
  assert.equal(malformed.status, 400);
  assert.equal(malformed.body.error, "Malformed JSON body");
});
