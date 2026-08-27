# QueueFlow

QueueFlow is a portfolio-scale distributed job execution platform. It accepts asynchronous work through a TypeScript/Express API, stores authoritative state in PostgreSQL, delivers execution messages through AWS SQS, runs work on independent Python workers, and stores results in Amazon S3.

The project demonstrates durable state, tenant isolation, quotas, transactional messaging, at-least-once delivery, idempotent job claiming, retries, dead-letter handling, worker leases and recovery, lightweight resource admission, and priority-aware dispatch.

QueueFlow is intentionally not a production HPC scheduler and does not execute real GPU workloads.

## Architecture

```text
Client / Python SDK
        |
        v
TypeScript / Express API
        |
        | one PostgreSQL transaction
        v
PostgreSQL: job + SUBMITTED event + outbox row
        |
        | capability- and priority-aware outbox publisher
        v
AWS SQS standard queue ---> dead-letter queue after 3 receives
        |
        | long polling and renewable visibility lease
        v
Independent Python workers
        |
        +-- atomic PostgreSQL job claim
        +-- CPU/memory/GPU admission check
        +-- workload execution
        +-- S3 result write
        +-- atomic completion/failure + lifecycle event
```

The API and workers scale independently. PostgreSQL is the source of truth; SQS is an at-least-once delivery mechanism, not the authoritative job-state store.

## Submission and transactional outbox

`POST /jobs` performs the following work in one PostgreSQL transaction:

1. Lock the authenticated tenant row so concurrent submissions cannot race the quota check.
2. Count the tenant's active jobs.
3. Insert the `QUEUED` job.
4. Insert its `SUBMITTED` lifecycle event.
5. Insert a unique `job_outbox` record containing the SQS payload.
6. Commit.

The HTTP request does not call SQS. A background publisher claims pending outbox records using `FOR UPDATE SKIP LOCKED`, sends them, and only then sets `published_at`. Failed sends are logged and retried with bounded exponential backoff.

There is an unavoidable crash window after SQS accepts a message but before `published_at` is recorded. The publisher may send that outbox record again after its lock expires. This is safe because workers use PostgreSQL to claim a job conditionally before executing it.

## Job lifecycle

Typical success:

```text
QUEUED / SUBMITTED
       |
       v
RUNNING / STARTED
       |
       v
COMPLETED / COMPLETED
```

Retry and terminal failure:

```text
QUEUED / SUBMITTED
       |
       v
RUNNING / STARTED (attempt 1)
       |
       v
RETRYING / RETRYING
       |
       v
RUNNING / STARTED (attempt 2 or 3)
       |
       +--------------------> COMPLETED / COMPLETED
       |
       +--------------------> FAILED / FAILED
```

Crash recovery can add a `RECOVERED` event and move a stale worker's `RUNNING` job to `RETRYING`. A queue publication failure does not create `SUBMISSION_FAILED`; the durable outbox stays pending and keeps retrying.

Important state changes pair the job update and lifecycle event in one short database transaction. Workload execution and S3 upload occur outside database transactions.

## Request validation

Job submissions are validated with Zod. Unknown fields and malformed JSON are rejected with HTTP 400 and field-level details.

Accepted values:

- `type`: `text_analysis`, `simulated_compute`, or `always_fail`
- `text`: required and non-empty for `text_analysis`; maximum 100,000 characters
- `cpu`: integer from 1 through 128
- `memoryMb`: integer from 1 through 1,048,576
- `gpu`: integer from 0 through 64
- `priority`: integer from 0 through 10

GPU is only an admission/scheduling dimension. QueueFlow does not allocate or execute on real GPUs.

## At-least-once delivery and idempotent claims

SQS Standard can deliver a message more than once. Receiving a message does not authorize execution. A worker first runs a conditional update equivalent to:

```sql
UPDATE jobs
SET status = 'RUNNING',
    attempt_count = attempt_count + 1,
    worker_id = $worker
WHERE id = $job
  AND status IN ('QUEUED', 'RETRYING')
  AND cpu_required <= $worker_cpu
  AND memory_required_mb <= $worker_memory
  AND gpu_required <= $worker_gpu
RETURNING ...;
```

Only the worker receiving a row may execute the workload. Messages for completed or missing jobs are acknowledged without execution. Messages for a currently running job are retained temporarily rather than stealing the active claim. Completion and failure updates also require the job to remain `RUNNING` and assigned to that worker, preventing a stale worker from overwriting a recovered job.

## Visibility lease renewal

The worker sets an initial SQS visibility timeout and starts a small lease-renewal thread while handling a message. The thread periodically:

- extends message visibility;
- refreshes the worker's `BUSY` heartbeat;
- logs renewal failures without terminating the workload.

The thread is stopped and joined in a `finally` block, so it does not survive the message. This reduces duplicate delivery during long execution, but cannot create an exactly-once guarantee if AWS or the worker is partitioned.

Configure the lease with:

```text
SQS_VISIBILITY_TIMEOUT_SECONDS=120
SQS_VISIBILITY_RENEWAL_SECONDS=30
```

The renewal interval must remain comfortably below the visibility timeout.

## Worker crash recovery

Each worker registers its capacity and updates `last_heartbeat`. The API runs a periodic recovery scan:

1. Lock workers whose heartbeat is older than `WORKER_STALE_AFTER_SECONDS`.
2. Mark them `OFFLINE`.
3. Move their still-assigned `RUNNING` jobs to `RETRYING` and clear the assignment.
4. Record a `RECOVERED` event in the same transaction.
5. Allow the original SQS message to become visible after its lease expires.

The default stale threshold is 120 seconds. It should be longer than normal heartbeat and lease-renewal intervals. A low value speeds recovery but increases the risk of treating a live worker with temporary database connectivity trouble as dead. The completion guard prevents that stale worker from committing after its claim has been recovered.

## Retry and DLQ behavior

An attempt is incremented only after a successful atomic claim. Workload errors transition the job to `RETRYING` until `max_attempts` is reached, then to `FAILED`. The message is not deleted on failure.

Retry visibility uses exponential backoff with small jitter:

```text
RETRY_BASE_DELAY_SECONDS=5
RETRY_MAX_DELAY_SECONDS=60
```

Terraform configures `maxReceiveCount = 3`. After repeated non-acknowledgement, SQS moves the message to the DLQ. The database attempt limit and SQS redrive count are both three and should be changed together. Malformed messages are retained and eventually reach the DLQ instead of creating an infinite loop.

## Resource-aware dispatch

The implementation deliberately keeps one SQS queue. Before publishing an outbox record, the lightweight dispatcher checks that at least one recently healthy worker advertises enough CPU, memory, and modeled GPU capacity. Unschedulable work remains durably `QUEUED` in PostgreSQL instead of immediately bouncing through SQS.

The worker repeats the capacity check atomically while claiming. A mismatch is not counted as a job attempt and receives a longer visibility delay.

This is smaller and easier to explain than a queue per hardware shape, but it cannot route a message to a particular suitable worker. In a highly heterogeneous pool, separate resource-class queues or a dedicated scheduler would be more appropriate.

## Priority behavior

Priority is an integer from 0 through 10. The outbox dispatcher chooses eligible unpublished jobs by priority, then adds an age boost so old low-priority work eventually competes with newer high-priority work. `PRIORITY_AGING_SECONDS` controls how quickly the boost grows.

This provides priority-aware dispatch, not strict priority execution. Once messages enter an SQS Standard queue, AWS does not guarantee their order. Strict priority would require a different queueing topology or a scheduler that assigns work directly.

## Supported workloads

### `text_analysis`

Returns word count, character count, sentence count, and the five most common words.

### `simulated_compute`

Runs for roughly ten seconds and persists progress at 10, 25, 50, 75, 90, and 100 percent.

### `always_fail`

Raises an intentional exception for exercising retries, failure events, and DLQ behavior.

## REST API

All `/jobs` and `/workers` routes require `x-api-key`. `/health` and `/metrics` are unauthenticated operational endpoints.

```bash
curl http://localhost:3001/health
```

```bash
curl -X POST http://localhost:3001/jobs \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: queueflow-demo-key' \
  -d '{
    "type": "text_analysis",
    "text": "QueueFlow validates and dispatches durable work.",
    "cpu": 1,
    "memoryMb": 256,
    "gpu": 0,
    "priority": 5
  }'
```

```bash
curl http://localhost:3001/jobs -H 'x-api-key: queueflow-demo-key'
curl http://localhost:3001/jobs/JOB_ID -H 'x-api-key: queueflow-demo-key'
curl http://localhost:3001/jobs/JOB_ID/events -H 'x-api-key: queueflow-demo-key'
curl http://localhost:3001/workers -H 'x-api-key: queueflow-demo-key'
curl http://localhost:3001/metrics
```

Authentication is tenant-scoped: job list, detail, and event queries always include the authenticated tenant ID. Worker capacity is currently cluster-wide.

## Database migrations

SQL migrations live in `apps/api/migrations`. The API acquires a PostgreSQL advisory lock and applies unapplied files at startup. Applied filenames are stored in `schema_migrations`, making concurrent API replica startup safe.

Run migrations manually with:

```bash
cd apps/api
DATABASE_URL=postgresql://queueflow:queueflow@localhost:5433/queueflow npm run migrate
```

Docker Compose sets `SEED_DEMO_TENANT=true`, which adds the local `queueflow-demo-key` tenant after migration. The migration itself contains no fixed credential, and Kubernetes does not enable this option. Production tenants and keys should be provisioned through a controlled administrative process.

## Local macOS setup

Prerequisites:

- Docker Desktop
- Node.js 22+
- Python 3.11+
- AWS CLI with access to the configured SQS queue and S3 bucket
- Terraform when provisioning AWS resources

Create local configuration:

```bash
cp .env.example .env
```

Replace `SQS_QUEUE_URL` and `S3_RESULTS_BUCKET` in `.env`, then verify AWS authentication:

```bash
aws sts get-caller-identity
```

Start the complete stack:

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f api worker postgres
```

The API automatically migrates PostgreSQL before becoming healthy. Open the dashboard at `http://localhost:4173`, select Live API, use `/api`, and use `queueflow-demo-key`. The API is at `http://localhost:3001`.

Stop without deleting PostgreSQL data:

```bash
docker compose down
```

Do not add `-v` unless you intend to delete the database volume.

## Terraform

Terraform provisions:

- encrypted SQS job and dead-letter queues;
- a three-receive redrive policy and explicit DLQ allow policy;
- a private, encrypted, versioned S3 results bucket.

Configure and apply:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# Set a globally unique results_bucket_name.
terraform init
terraform plan
terraform apply
```

Copy the output queue URL and bucket name into the root `.env`. Terraform state can contain sensitive infrastructure data and must remain outside version control.

## Kubernetes

The checked-in deployment contains no database password, AWS access key, or developer-specific host path. Create the non-secret configuration from the template:

```bash
cp k8s/config.example.yaml /tmp/queueflow-config.yaml
# Replace the queue URL and bucket name.
kubectl apply -f /tmp/queueflow-config.yaml
```

Create the database secret without committing it:

```bash
kubectl create secret generic queueflow-secrets \
  --from-literal=DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/queueflow'
```

Then deploy:

```bash
kubectl apply -f k8s/queueflow.yaml
```

The pods use the `queueflow` Kubernetes service account and the AWS SDK default credential chain. On production Kubernetes, associate that service account with workload identity such as EKS Pod Identity or IRSA. Do not mount a developer's `~/.aws` directory into production pods.

## Graceful shutdown

Workers handle SIGTERM and SIGINT by stopping new polls, allowing the current message handler to finish when the platform grace period permits, stopping its visibility-renewal thread, marking the worker offline, and exiting. Docker Compose and Kubernetes both provide a 60-second termination grace period, which is longer than the 20-second SQS long poll. If a workload cannot finish before forced termination, its SQS lease expires and stale-worker recovery makes the database job retryable.

## Observability

The API and worker emit newline-delimited JSON logs with fields such as `job_id`, `worker_id`, `attempt`, transition status, retry delay, outbox ID, and error. Logs intentionally go to stdout/stderr for collection by Docker or Kubernetes.

`GET /metrics` exposes Prometheus text metrics for current job counts by status, active workers, submissions observed by that API process, outbox failures, scan failures, and recoveries. In-memory counters are per API replica and reset at restart; durable job/worker gauges come from PostgreSQL.

## Tests and CI

Run API checks:

```bash
cd apps/api
npm ci
npm run build
npm test
```

Set `TEST_DATABASE_URL` to enable the PostgreSQL integration test. Without it, that one test is explicitly skipped.

Run worker checks:

```bash
cd workers/processor
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/ruff check worker.py tests
.venv/bin/python -m py_compile worker.py
.venv/bin/python -m unittest discover -s tests -v
```

GitHub Actions starts PostgreSQL, applies migrations, builds and tests the TypeScript backend, then lints, compiles, and tests the Python worker. AWS clients are mocked in tests, and CI does not require AWS credentials.

## Environment variables

API:

```text
DATABASE_URL                         required
PORT                                 default 3001
CORS_ORIGIN                          optional comma-separated allowlist
AWS_REGION                           default us-east-1
SQS_QUEUE_URL                        required for publication
OUTBOX_POLL_INTERVAL_MS              default 1000
OUTBOX_LOCK_TIMEOUT_SECONDS          default 30
OUTBOX_RETRY_BASE_MS                 default 1000
OUTBOX_RETRY_MAX_MS                  default 60000
WORKER_STALE_AFTER_SECONDS           default 120
WORKER_RECOVERY_INTERVAL_MS          default 30000
PRIORITY_AGING_SECONDS               default 60
SEED_DEMO_TENANT                     default false; local development only
```

Worker:

```text
DATABASE_URL                         required
AWS_REGION                           default us-east-1
SQS_QUEUE_URL                        required
S3_RESULTS_BUCKET                    optional; results remain in PostgreSQL if unset
WORKER_ID                            generated at startup when unset
WORKER_NAME                          default queueflow-worker
WORKER_CPU_CAPACITY                  default 4
WORKER_MEMORY_MB                     default 8192
WORKER_GPU_CAPACITY                  default 0
SQS_VISIBILITY_TIMEOUT_SECONDS       default 120
SQS_VISIBILITY_RENEWAL_SECONDS       default 30
RETRY_BASE_DELAY_SECONDS             default 5
RETRY_MAX_DELAY_SECONDS              default 60
RESOURCE_MISMATCH_DELAY_SECONDS      default 30
```

## Repository layout

```text
apps/api/                    Express API, outbox publisher, recovery, migrations, tests
apps/web/                    Optional React dashboard
workers/processor/           Python worker and tests
sdk/python/                  Lightweight Python client
infra/terraform/             SQS, DLQ, and S3 infrastructure
k8s/                        Kubernetes deployments and config template
docker-compose.yml           Local PostgreSQL/API/worker/web stack
.github/workflows/ci.yml     Backend and worker CI
```

## Known limitations

- QueueFlow does not execute real GPU workloads. GPU is only an integer capacity requirement.
- This is not a replacement for Slurm, Ray, Kubernetes Kueue, or a production workflow engine.
- The scheduler is an intentionally lightweight outbox dispatcher. SQS Standard prevents strict priority ordering and targeted worker assignment.
- Capability gating only proves that a suitable healthy worker existed at dispatch time. Another unsuitable worker can still receive the message.
- PostgreSQL and SQS together do not provide exactly-once execution. Correctness depends on conditional claims and idempotent state transitions; arbitrary external workload side effects are not automatically idempotent.
- Recovery uses heartbeat age as evidence of failure. Network partitions force a tradeoff between recovery speed and false positives.
- Results are written to S3 before the completion transaction. A crash in between can leave an object for a job that later retries; the deterministic key makes later writes replace the logical result.
- The outbox and recovery loops run inside every API replica. Database row locks make this safe, but a dedicated control-plane process would be easier to scale and observe at high throughput.
- API keys are stored directly in the current schema. Production systems should store hashed credentials, support rotation, and add rate limiting and audit logs.
- `/metrics` is unauthenticated and should be restricted by network policy or a private service in production.
- Local Docker/Kubernetes testing is not equivalent to operating a production Kubernetes cluster. The manifests omit ingress, network policies, pod disruption budgets, autoscaling, managed PostgreSQL, backups, and full monitoring.
- Automated tests mock AWS behavior. A separate, credentialed staging environment is still needed to validate real SQS visibility, redrive, IAM, and S3 behavior.

## Toward heterogeneous GPU/HPC workloads

A serious heterogeneous platform would need resource-class or per-capability queues, a centralized scheduler with reservations and fairness, real GPU discovery and device allocation, topology awareness, preemption, gang scheduling, workload isolation, artifact staging, cancellation, idempotent user workloads, admission control, autoscaling, durable scheduler leadership, richer metrics/tracing, and production workload identity. QueueFlow models the surrounding ideas without claiming to implement those systems.
