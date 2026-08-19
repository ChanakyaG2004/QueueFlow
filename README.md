# QueueFlow

QueueFlow is a distributed job execution platform for submitting, processing, tracking, and storing asynchronous workloads across independent workers.

It uses a **TypeScript/Express control plane**, **Python workers**, **PostgreSQL**, **AWS SQS**, **Amazon S3**, **Docker**, **Kubernetes**, and **Terraform**.

QueueFlow includes job lifecycle tracking, retries, dead-letter queue handling, worker registration and heartbeats, resource-aware worker admission, API-key authentication, tenant quotas, S3-backed results, a Python SDK, and CI with GitHub Actions.

---

## Architecture

```text
                       ┌──────────────────────┐
                       │      Client          │
                       │ REST API / Python SDK│
                       └──────────┬───────────┘
                                  │
                                  │ HTTP
                                  ▼
                       ┌──────────────────────┐
                       │    QueueFlow API     │
                       │ TypeScript + Express │
                       └───────┬──────┬───────┘
                               │      │
                    Job State  │      │ Job Message
                               │      │
                               ▼      ▼
                     ┌────────────┐  ┌──────────────┐
                     │ PostgreSQL │  │   AWS SQS    │
                     └────────────┘  └──────┬───────┘
                                            │
                                            │ Poll
                                            ▼
                                  ┌────────────────────┐
                                  │   Python Workers   │
                                  │                    │
                                  │ Worker 1           │
                                  │ Worker 2           │
                                  │ ...                │
                                  └─────────┬──────────┘
                                            │
                                            │ Result
                                            ▼
                                      ┌──────────┐
                                      │  AWS S3  │
                                      └──────────┘
```

The API and workers are separate services and can scale independently.

A job submitted to QueueFlow follows this path:

```text
Client
  ↓
QueueFlow API
  ↓
PostgreSQL + AWS SQS
  ↓
Python Worker
  ↓
Amazon S3
  ↓
Completed Job State
```

---

## Features

### Distributed Job Processing

Clients submit jobs through the QueueFlow API.

The API stores durable job state in PostgreSQL and publishes an execution message to AWS SQS.

Independent Python workers continuously poll SQS and process available jobs.

This separates job submission from execution and allows API and worker capacity to scale independently.

---

### Durable Job State

PostgreSQL stores persistent information for each job, including:

- job ID
- tenant
- job type
- status
- progress
- attempt count
- maximum attempts
- CPU requirement
- memory requirement
- GPU requirement
- priority
- assigned worker
- creation time
- start time
- completion time
- result
- error
- S3 result location

---

### Job Lifecycle Events

QueueFlow maintains a persistent event history for every job.

A successful job can follow:

```text
SUBMITTED
    ↓
STARTED
    ↓
COMPLETED
```

A failing job may follow:

```text
SUBMITTED
    ↓
STARTED
    ↓
RETRYING
    ↓
STARTED
    ↓
FAILED
```

Events can include metadata such as:

- attempt number
- maximum attempts
- worker ID
- requested resources
- result location
- failure information

Example:

```json
[
  {
    "event_type": "SUBMITTED",
    "metadata": {
      "cpu": 1,
      "memoryMb": 256,
      "gpu": 0,
      "priority": 5
    }
  },
  {
    "event_type": "STARTED",
    "metadata": {
      "attempt": 1,
      "max_attempts": 3,
      "worker_id": "..."
    }
  },
  {
    "event_type": "COMPLETED",
    "metadata": {
      "attempt": 1,
      "worker_id": "...",
      "result_location": "s3://..."
    }
  }
]
```

---

### Retry and Failure Handling

Workers track execution attempts for every job.

Jobs contain:

```text
attempt_count
max_attempts
```

When execution fails, QueueFlow records the error and updates the job state.

AWS SQS redelivery is used to retry failed messages.

---

### Dead-Letter Queue

QueueFlow uses an AWS SQS dead-letter queue.

The main SQS queue is configured with a redrive policy and a maximum receive count.

Messages that repeatedly fail are moved to the dead-letter queue instead of being retried forever.

This provides a separate location for inspecting jobs that could not be successfully processed.

---

### Worker Registry

Workers register themselves in PostgreSQL when they start.

Each worker tracks:

- worker ID
- worker name
- status
- CPU capacity
- memory capacity
- GPU capacity
- current job
- last heartbeat
- creation time

Each Kubernetes worker replica receives a unique worker ID.

---

### Worker Heartbeats

Workers periodically update their heartbeat in PostgreSQL.

The API can use heartbeat timestamps to determine whether registered workers are currently healthy.

Workers transition between states such as:

```text
IDLE
BUSY
OFFLINE
```

---

### Resource-Aware Worker Admission

Jobs can declare compute requirements when submitted.

Example:

```json
{
  "cpu": 1,
  "memoryMb": 256,
  "gpu": 0
}
```

Workers also advertise their own capacity.

Example:

```text
CPU:     4
Memory:  8192 MB
GPU:     0
```

Before executing a job, a worker compares the job requirements against its configured capacity.

If the worker cannot satisfy the request, it does not execute the workload.

GPU capacity is currently modeled for scheduling/admission testing. QueueFlow does not currently execute real GPU workloads.

---

### API-Key Authentication

Protected QueueFlow endpoints require an API key.

Example:

```http
x-api-key: queueflow-demo-key
```

API keys are associated with tenants in PostgreSQL.

---

### Tenant Isolation

Every job belongs to a tenant.

Authenticated requests only return jobs belonging to the tenant associated with the supplied API key.

This prevents one tenant from reading another tenant's jobs.

---

### Active Job Quotas

Each tenant has a configurable maximum number of active jobs.

Before accepting another job, QueueFlow counts the tenant's jobs currently in active states such as:

```text
QUEUED
RUNNING
RETRYING
```

If the quota has been reached, the API rejects the submission.

---

### Amazon S3 Result Storage

Completed job results are persisted to Amazon S3.

Results use a structure such as:

```text
s3://<bucket>/jobs/<job-id>/result.json
```

The corresponding S3 URI is stored with the PostgreSQL job record.

Example:

```text
s3://queueflow-results-819379976838-us-east-1/jobs/6c6609cb-9dcc-40c5-8766-ed7000a8c98a/result.json
```

---

### Docker

The API and worker are independently containerized.

```text
queueflow-api
queueflow-worker
```

Docker Compose can be used to run the local development stack.

---

### Kubernetes

QueueFlow can run the API and workers as replicated Kubernetes deployments.

The current local development configuration runs:

```text
2 API replicas
2 worker replicas
```

The API is exposed internally through a Kubernetes Service.

Readiness probes are used to verify API health before Kubernetes sends traffic to a pod.

---

### Terraform

QueueFlow infrastructure is codified with Terraform.

Terraform configuration currently manages:

- AWS SQS job queue
- AWS SQS dead-letter queue
- SQS redrive policy
- Amazon S3 results bucket

Existing AWS resources were imported into Terraform state so that their configuration can be managed declaratively.

---

### Python SDK

QueueFlow includes a lightweight Python SDK for:

- job submission
- job lookup
- event lookup
- polling until completion

Example:

```python
from queueflow import QueueFlowClient

client = QueueFlowClient(
    base_url="http://localhost:3001",
    api_key="queueflow-demo-key"
)

job = client.submit(
    job_type="text_analysis",
    text="QueueFlow SDK submission works"
)

print("Submitted:", job["id"])

completed = client.wait(job["id"])

print("Status:", completed["status"])
print("Result:", completed["result"])
print("S3:", completed["result_location"])
```

Example output:

```text
Submitted: 4f96c3a0-bb28-4d3e-89bf-bbff6689712d
Status: COMPLETED
Result: {
    'top_words': [
        ['queueflow', 1],
        ['sdk', 1],
        ['submission', 1],
        ['works', 1]
    ],
    'word_count': 4,
    'sentence_count': 1,
    'character_count': 31
}
S3: s3://queueflow-results-819379976838-us-east-1/jobs/4f96c3a0-bb28-4d3e-89bf-bbff6689712d/result.json
```

---

### Continuous Integration

QueueFlow uses GitHub Actions for CI.

On pushes to `main` and pull requests, GitHub Actions runs two jobs.

#### API

```text
Install Node.js dependencies
↓
Compile TypeScript
```

#### Worker

```text
Install Python dependencies
↓
Compile-check worker.py
```

Both the API and worker CI jobs have been validated successfully.

---

## Tech Stack

### Control Plane

- TypeScript
- Node.js
- Express

### Workers

- Python
- boto3
- psycopg2

### Database

- PostgreSQL

### AWS

- Amazon SQS
- Amazon S3

### Infrastructure

- Docker
- Docker Compose
- Kubernetes
- Terraform

### Developer Tooling

- GitHub Actions
- AWS CLI
- kubectl

### Client

- REST API
- Python SDK

---

## Project Structure

```text
QueueFlow/
│
├── apps/
│   └── api/
│       ├── src/
│       │   ├── index.ts
│       │   ├── db.ts
│       │   └── sqs.ts
│       ├── Dockerfile
│       ├── package.json
│       ├── package-lock.json
│       └── tsconfig.json
│
├── workers/
│   └── processor/
│       ├── worker.py
│       ├── requirements.txt
│       └── Dockerfile
│
├── sdk/
│   └── python/
│       ├── queueflow.py
│       └── example.py
│
├── infra/
│   └── terraform/
│       ├── main.tf
│       └── .terraform.lock.hcl
│
├── k8s/
│   └── queueflow.yaml
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

---

# Supported Job Types

QueueFlow currently includes several workloads for testing the distributed execution platform.

## Text Analysis

Analyzes a text payload and returns basic statistics.

Example request:

```json
{
  "type": "text_analysis",
  "text": "QueueFlow Kubernetes end to end test."
}
```

Example result:

```json
{
  "top_words": [
    ["end", 2],
    ["queueflow", 1],
    ["kubernetes", 1],
    ["to", 1],
    ["test", 1]
  ],
  "word_count": 6,
  "sentence_count": 1,
  "character_count": 37
}
```

---

## Simulated Compute

Runs a longer simulated workload and updates job progress over time.

Example progression:

```text
10%
25%
50%
75%
90%
100%
```

This workload is useful for testing:

- asynchronous execution
- progress updates
- longer-running jobs
- worker state transitions

---

## Intentional Failure

The `always_fail` job intentionally throws an exception.

It is used to test:

- retries
- attempt tracking
- error persistence
- lifecycle events
- SQS redelivery
- dead-letter queue behavior

---

# REST API

## Health Check

```bash
curl http://localhost:3001/health
```

Example response:

```json
{
  "status": "ok",
  "database": "connected"
}
```

---

## Submit a Job

```bash
curl -X POST http://localhost:3001/jobs \
  -H "Content-Type: application/json" \
  -H "x-api-key: queueflow-demo-key" \
  -d '{
    "type": "text_analysis",
    "text": "QueueFlow distributed execution test.",
    "cpu": 1,
    "memoryMb": 256,
    "gpu": 0,
    "priority": 5
  }'
```

Example response:

```json
{
  "id": "6c6609cb-9dcc-40c5-8766-ed7000a8c98a",
  "type": "text_analysis",
  "status": "QUEUED",
  "progress": 0,
  "attempt_count": 0,
  "max_attempts": 3,
  "cpu_required": 1,
  "memory_required_mb": 256,
  "gpu_required": 0,
  "priority": 5
}
```

---

## Get Job

```bash
curl http://localhost:3001/jobs/<job-id> \
  -H "x-api-key: queueflow-demo-key"
```

Example completed job:

```json
{
  "id": "6c6609cb-9dcc-40c5-8766-ed7000a8c98a",
  "type": "text_analysis",
  "status": "COMPLETED",
  "progress": 100,
  "attempt_count": 1,
  "max_attempts": 3,
  "worker_id": "8c38e14a-dab9-4951-bf24-c0db385e2161",
  "result_location": "s3://queueflow-results-819379976838-us-east-1/jobs/6c6609cb-9dcc-40c5-8766-ed7000a8c98a/result.json"
}
```

---

## Get Job Events

```bash
curl http://localhost:3001/jobs/<job-id>/events \
  -H "x-api-key: queueflow-demo-key"
```

---

## List Jobs

```bash
curl http://localhost:3001/jobs \
  -H "x-api-key: queueflow-demo-key"
```

---

## List Workers

```bash
curl http://localhost:3001/workers \
  -H "x-api-key: queueflow-demo-key"
```

This endpoint exposes registered workers and their health/capacity information.

---

# Running QueueFlow Locally

## Prerequisites

Install:

- Docker Desktop
- Node.js 22+
- Python 3.11+
- AWS CLI
- Terraform
- kubectl

You also need an AWS account with access to SQS and S3.

---

## AWS Authentication

Configure the AWS CLI:

```bash
aws configure
```

Verify authentication:

```bash
aws sts get-caller-identity
```

Never commit AWS credentials to the repository.

---

## Environment Variables

The services use configuration such as:

```text
DATABASE_URL
AWS_REGION
SQS_QUEUE_URL
S3_RESULTS_BUCKET
```

Worker-specific variables include:

```text
WORKER_NAME
WORKER_CPU_CAPACITY
WORKER_MEMORY_MB
WORKER_GPU_CAPACITY
```

Local secrets should be stored outside Git.

---

# Docker Compose

Start the local stack:

```bash
docker compose up --build
```

The API will be available at:

```text
http://localhost:3001
```

Check health:

```bash
curl http://localhost:3001/health
```

---

# Kubernetes

QueueFlow has also been tested locally on Kubernetes using Docker Desktop.

Build the images:

```bash
docker build -t queueflow-api:local apps/api
```

```bash
docker build -t queueflow-worker:local workers/processor
```

Apply the Kubernetes resources:

```bash
kubectl apply -f k8s/queueflow.yaml
```

Check the deployments:

```bash
kubectl get pods
```

A healthy local deployment should show two API replicas and two worker replicas:

```text
queueflow-api-...       1/1   Running
queueflow-api-...       1/1   Running
queueflow-worker-...    1/1   Running
queueflow-worker-...    1/1   Running
```

Forward the API service to the local machine:

```bash
kubectl port-forward service/queueflow-api 3002:3001
```

Then verify:

```bash
curl http://localhost:3002/health
```

---

## Kubernetes and AWS Credentials

The current Kubernetes setup is designed for local development.

For local testing, AWS credentials can be made available to the API and worker containers from the developer environment.

Production deployments should **not** use developer credentials.

A production deployment should use workload-based AWS authentication such as:

- IAM roles
- service accounts
- EKS Pod Identity
- IRSA

depending on the deployment environment.

---

# Terraform

Terraform files are located in:

```text
infra/terraform/
```

Initialize Terraform:

```bash
cd infra/terraform
terraform init
```

Preview infrastructure changes:

```bash
terraform plan
```

Existing QueueFlow AWS resources have been imported into Terraform state so that their infrastructure configuration can be maintained declaratively.

Terraform state files and downloaded providers are intentionally excluded from Git.

---

# End-to-End Validation

QueueFlow has been validated through the complete execution path.

A Kubernetes API request was submitted with:

```json
{
  "type": "text_analysis",
  "text": "QueueFlow Kubernetes end to end test.",
  "cpu": 1,
  "memoryMb": 256,
  "gpu": 0,
  "priority": 5
}
```

The API initially returned:

```text
status: QUEUED
```

A Python worker then consumed the message and the job reached:

```text
status: COMPLETED
progress: 100
attempt_count: 1
```

The job contained an assigned worker ID and an S3 result location.

Lifecycle history recorded:

```text
SUBMITTED
STARTED
COMPLETED
```

The final result was successfully persisted to Amazon S3.

This validated:

```text
Authenticated Client
        ↓
Kubernetes API
        ↓
PostgreSQL
        ↓
AWS SQS
        ↓
Python Worker
        ↓
Amazon S3
        ↓
Persistent Job + Event State
```

---

# Current Limitations

QueueFlow is currently a portfolio-scale distributed systems implementation rather than a production scheduler.

Current limitations include:

- GPU capacity is modeled but real GPU execution is not implemented
- workers use resource-aware admission rather than a centralized heterogeneous scheduler
- job priority is stored but AWS SQS Standard queues do not guarantee priority ordering
- PostgreSQL currently runs outside the Kubernetes cluster in the local development setup
- Kubernetes AWS authentication is currently configured for local development rather than production workload identity
- the Python SDK is local to the repository and is not currently published to PyPI

These are potential areas for future development.

---

# Roadmap

Planned improvements include:

- [ ] Web dashboard for jobs, workers, events, and system health
- [ ] Live job progress visualization
- [ ] Worker capacity dashboard
- [ ] Retry and failure inspection UI
- [ ] Dead-letter queue visibility
- [ ] Centralized resource-aware scheduling
- [ ] Priority-aware queueing
- [ ] Real GPU worker support
- [ ] Prometheus metrics
- [ ] Grafana dashboards
- [ ] Distributed tracing
- [ ] Structured logging
- [ ] Rate limiting
- [ ] Improved tenant management
- [ ] PostgreSQL deployment inside Kubernetes
- [ ] Production AWS workload identity
- [ ] Helm chart
- [ ] Python package publishing
- [ ] Automated integration tests

---

# Why QueueFlow?

QueueFlow was built to explore the infrastructure behind distributed compute platforms:

- How should applications submit long-running work without blocking an API request?
- How can workers execute jobs independently?
- How should job state survive process restarts?
- What happens when a worker fails?
- How can failed jobs be retried safely?
- How can repeatedly failing work be isolated?
- How can worker capacity and health be tracked?
- How can results be stored independently of the worker that produced them?
- How can the same services be containerized and replicated?

The project focuses on the systems surrounding compute workloads rather than the workload itself.

---

# Status

The QueueFlow backend and distributed execution infrastructure are functional.

Validated components include:

- TypeScript/Express API
- Python workers
- PostgreSQL job state
- AWS SQS job delivery
- AWS SQS dead-letter queue
- Amazon S3 result storage
- retry/failure handling
- lifecycle event logging
- worker registration
- worker heartbeats
- resource-aware admission
- API-key authentication
- tenant isolation
- tenant quotas
- Docker
- Kubernetes replicas
- Terraform infrastructure management
- Python SDK
- GitHub Actions CI

A web frontend/dashboard is planned next.
