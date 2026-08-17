import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { pool } from "./db.js";
import crypto from "crypto";
import { sendJobToQueue } from "./sqs.js";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

app.get("/health", async (req, res) => {
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

const port = process.env.PORT || 3001;
app.post("/jobs", async (req, res) => {
  try {
    const { type, text } = req.body;

    if (!type) {
      return res.status(400).json({
        error: "Job type is required",
      });
    }

    const id = crypto.randomUUID();

    const result = await pool.query(
      `
      INSERT INTO jobs (id, type, status)
      VALUES ($1, $2, $3)
      RETURNING *
      `,
      [id, type, "QUEUED"]
    );

    await sendJobToQueue(id, type, text);

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to create job",
    });
  }
});

app.get("/jobs", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM jobs ORDER BY created_at DESC"
    );

    res.json(result.rows);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to get jobs",
    });
  }
});

app.get("/jobs/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const result = await pool.query(
      "SELECT * FROM jobs WHERE id = $1",
      [id]
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


app.listen(port, () => {
  console.log(`QueueFlow API running on http://localhost:${port}`);
});