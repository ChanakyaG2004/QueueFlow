import express from "express";
import type { Request, Response, NextFunction } from "express";import cors from "cors";
import dotenv from "dotenv";
import crypto from "crypto";

import { pool } from "./db.js";
import { sendJobToQueue } from "./sqs.js";

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

app.use(cors());
app.use(express.json());

type AuthedRequest = Request & {
  tenantId?: string;
};

async function authenticate(
  req: AuthedRequest,
  res: Response,
  next: NextFunction
) {
  const apiKey = req.header("x-api-key");

  if (!apiKey) {
    return res.status(401).json({
      error: "API key required",
    });
  }

  const result = await pool.query(
    `
    SELECT id
    FROM tenants
    WHERE api_key = $1
    `,
    [apiKey]
  );

  if (result.rows.length === 0) {
    return res.status(401).json({
      error: "Invalid API key",
    });
  }

  req.tenantId = result.rows[0].id;
  next();
}

app.get("/health", async (_req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");

    res.json({
      status: "ok",
      database: "connected",
      time: result.rows[0].now,
    });
  } catch (error) {
    console.error(error);

    res.status(500).json({
      status: "error",
      database: "disconnected",
    });
  }
});

app.use("/jobs", authenticate);
app.use("/workers", authenticate);

app.post("/jobs", async (req: AuthedRequest, res) => {
  try {
    const {
      type,
      text,
      cpu = 1,
      memoryMb = 256,
      gpu = 0,
      priority = 0,
    } = req.body;

    if (!type) {
      return res.status(400).json({
        error: "Job type is required",
      });
    }

    const quotaResult = await pool.query(
      `
      SELECT
        t.max_active_jobs,
        COUNT(j.id)::int AS active_jobs
      FROM tenants t
      LEFT JOIN jobs j
        ON j.tenant_id = t.id
        AND j.status IN (
          'QUEUED',
          'RUNNING',
          'RETRYING'
        )
      WHERE t.id = $1
      GROUP BY t.id
      `,
      [req.tenantId]
    );

    const quota = quotaResult.rows[0];

    if (
      quota &&
      quota.active_jobs >= quota.max_active_jobs
    ) {
      return res.status(429).json({
        error: "Active job quota exceeded",
      });
    }

    const id = crypto.randomUUID();

    const result = await pool.query(
      `
      INSERT INTO jobs (
        id,
        tenant_id,
        type,
        status,
        cpu_required,
        memory_required_mb,
        gpu_required,
        priority
      )
      VALUES (
        $1, $2, $3, 'QUEUED',
        $4, $5, $6, $7
      )
      RETURNING *
      `,
      [
        id,
        req.tenantId,
        type,
        cpu,
        memoryMb,
        gpu,
        priority,
      ]
    );

    await pool.query(
      `
      INSERT INTO job_events (
        job_id,
        event_type,
        message,
        metadata
      )
      VALUES ($1, 'SUBMITTED', $2, $3)
      `,
      [
        id,
        "Job submitted to QueueFlow",
        JSON.stringify({
          cpu,
          memoryMb,
          gpu,
          priority,
        }),
      ]
    );

    try {
      await sendJobToQueue(id, type, text);
    } catch (queueError) {
      await pool.query(
        `
        UPDATE jobs
        SET status = 'SUBMISSION_FAILED',
            error = $1
        WHERE id = $2
        `,
        [
          queueError instanceof Error
            ? queueError.message
            : "Queue submission failed",
          id,
        ]
      );

      throw queueError;
    }

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to create job",
    });
  }
});

app.get("/jobs", async (req: AuthedRequest, res) => {
  try {
    const result = await pool.query(
      `
      SELECT *
      FROM jobs
      WHERE tenant_id = $1
      ORDER BY priority DESC, created_at DESC
      `,
      [req.tenantId]
    );

    res.json(result.rows);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to get jobs",
    });
  }
});

app.get("/jobs/:id/events", async (req: AuthedRequest, res) => {
  try {
    const result = await pool.query(
      `
      SELECT e.*
      FROM job_events e
      JOIN jobs j
        ON j.id = e.job_id
      WHERE e.job_id = $1
        AND j.tenant_id = $2
      ORDER BY e.created_at ASC
      `,
      [req.params.id, req.tenantId]
    );

    res.json(result.rows);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to get job events",
    });
  }
});

app.get("/jobs/:id", async (req: AuthedRequest, res) => {
  try {
    const result = await pool.query(
      `
      SELECT *
      FROM jobs
      WHERE id = $1
        AND tenant_id = $2
      `,
      [req.params.id, req.tenantId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        error: "Job not found",
      });
    }

    res.json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to get job",
    });
  }
});

app.get("/workers", async (_req, res) => {
  try {
    const result = await pool.query(
      `
      SELECT
        *,
        CASE
          WHEN last_heartbeat >
            NOW() - INTERVAL '60 seconds'
          THEN true
          ELSE false
        END AS healthy
      FROM workers
      ORDER BY created_at DESC
      `
    );

    res.json(result.rows);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to get workers",
    });
  }
});

app.listen(port, () => {
  console.log(
    `QueueFlow API running on http://localhost:${port}`
  );
});