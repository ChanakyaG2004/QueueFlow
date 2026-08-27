CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tenants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  api_key TEXT NOT NULL UNIQUE,
  max_active_jobs INTEGER NOT NULL DEFAULT 10 CHECK (max_active_jobs > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'QUEUED',
  progress INTEGER NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
  cpu_required INTEGER NOT NULL DEFAULT 1 CHECK (cpu_required > 0),
  memory_required_mb INTEGER NOT NULL DEFAULT 256 CHECK (memory_required_mb > 0),
  gpu_required INTEGER NOT NULL DEFAULT 0 CHECK (gpu_required >= 0),
  priority INTEGER NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 10),
  worker_id UUID,
  result JSONB,
  result_location TEXT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS workers (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'IDLE',
  cpu_capacity INTEGER NOT NULL CHECK (cpu_capacity > 0),
  memory_capacity_mb INTEGER NOT NULL CHECK (memory_capacity_mb > 0),
  gpu_capacity INTEGER NOT NULL DEFAULT 0 CHECK (gpu_capacity >= 0),
  current_job_id UUID,
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_events (
  id BIGSERIAL PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  message TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS job_outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id UUID NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
  payload JSONB NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_at TIMESTAMPTZ,
  locked_by TEXT,
  published_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_migrations (
  name TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS jobs_tenant_status_idx
  ON jobs (tenant_id, status);

CREATE INDEX IF NOT EXISTS jobs_dispatch_idx
  ON jobs (status, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS job_events_job_created_idx
  ON job_events (job_id, created_at);

CREATE INDEX IF NOT EXISTS workers_heartbeat_idx
  ON workers (last_heartbeat);

CREATE INDEX IF NOT EXISTS job_outbox_pending_idx
  ON job_outbox (next_attempt_at, created_at)
  WHERE published_at IS NULL;
