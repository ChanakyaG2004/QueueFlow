import express from "express";
import type { NextFunction, Request, Response } from "express";
import cors from "cors";
import type { Pool } from "pg";
import { ZodError } from "zod";
import { createJob, QuotaExceededError } from "./job-service.js";
import { logger } from "./logger.js";
import { incrementMetric, renderCounters } from "./metrics.js";
import { createJobSchema } from "./validation.js";

export type AuthedRequest = Request & { tenantId?: string };

export function createApp(pool: Pool) {
  const app = express();
  const allowedOrigins = process.env.CORS_ORIGIN?.split(",").map((origin) => origin.trim());
  app.use(cors({ origin: allowedOrigins?.length ? allowedOrigins : true }));
  app.use(express.json({ limit: "128kb" }));

  async function authenticate(req: AuthedRequest, res: Response, next: NextFunction) {
    try {
      const apiKey = req.header("x-api-key");
      if (!apiKey) return res.status(401).json({ error: "API key required" });
      const result = await pool.query("SELECT id FROM tenants WHERE api_key = $1", [apiKey]);
      if (!result.rowCount) return res.status(401).json({ error: "Invalid API key" });
      req.tenantId = result.rows[0].id;
      next();
    } catch (error) {
      next(error);
    }
  }

  app.get("/health", async (_req, res) => {
    try {
      const result = await pool.query("SELECT NOW()");
      res.json({ status: "ok", database: "connected", time: result.rows[0].now });
    } catch (error) {
      logger.error("health check failed", { error: error instanceof Error ? error.message : String(error) });
      res.status(503).json({ status: "error", database: "disconnected" });
    }
  });

  app.get("/metrics", async (_req, res, next) => {
    try {
      const [jobs, workers] = await Promise.all([
        pool.query<{ status: string; count: number }>("SELECT status, COUNT(*)::int AS count FROM jobs GROUP BY status"),
        pool.query<{ count: number }>(
          `SELECT COUNT(*)::int AS count FROM workers
           WHERE status <> 'OFFLINE' AND last_heartbeat > NOW() - INTERVAL '90 seconds'`,
        ),
      ]);
      const lines = [renderCounters()];
      for (const row of jobs.rows) lines.push(`queueflow_jobs{status="${row.status}"} ${row.count}`);
      lines.push(`queueflow_active_workers ${workers.rows[0]?.count ?? 0}`);
      res.type("text/plain; version=0.0.4").send(`${lines.filter(Boolean).join("\n")}\n`);
    } catch (error) {
      next(error);
    }
  });

  app.use("/jobs", authenticate);
  app.use("/workers", authenticate);

  app.post("/jobs", async (req: AuthedRequest, res, next) => {
    try {
      const input = createJobSchema.parse(req.body);
      const job = await createJob(pool, req.tenantId!, input);
      incrementMetric("jobs_submitted");
      logger.info("job submitted", { job_id: job.id, tenant_id: req.tenantId, priority: job.priority });
      res.status(201).json(job);
    } catch (error) {
      if (error instanceof ZodError) {
        return res.status(400).json({
          error: "Invalid job payload",
          details: error.issues.map((issue) => ({ path: issue.path.join("."), message: issue.message })),
        });
      }
      if (error instanceof QuotaExceededError) return res.status(429).json({ error: error.message });
      next(error);
    }
  });

  app.get("/jobs", async (req: AuthedRequest, res, next) => {
    try {
      const result = await pool.query(
        `SELECT * FROM jobs WHERE tenant_id = $1
         ORDER BY priority DESC, created_at DESC`,
        [req.tenantId],
      );
      res.json(result.rows);
    } catch (error) {
      next(error);
    }
  });

  app.get("/jobs/:id/events", async (req: AuthedRequest, res, next) => {
    try {
      const result = await pool.query(
        `SELECT e.* FROM job_events e JOIN jobs j ON j.id = e.job_id
         WHERE e.job_id = $1 AND j.tenant_id = $2 ORDER BY e.created_at ASC`,
        [req.params.id, req.tenantId],
      );
      res.json(result.rows);
    } catch (error) {
      next(error);
    }
  });

  app.get("/jobs/:id", async (req: AuthedRequest, res, next) => {
    try {
      const result = await pool.query("SELECT * FROM jobs WHERE id = $1 AND tenant_id = $2", [req.params.id, req.tenantId]);
      if (!result.rowCount) return res.status(404).json({ error: "Job not found" });
      res.json(result.rows[0]);
    } catch (error) {
      next(error);
    }
  });

  app.get("/workers", async (_req, res, next) => {
    try {
      const result = await pool.query(
        `SELECT *, last_heartbeat > NOW() - INTERVAL '60 seconds' AS healthy
         FROM workers ORDER BY created_at DESC`,
      );
      res.json(result.rows);
    } catch (error) {
      next(error);
    }
  });

  app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (error instanceof SyntaxError && "body" in error) return res.status(400).json({ error: "Malformed JSON body" });
    logger.error("request failed", { error: error instanceof Error ? error.message : String(error) });
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}
