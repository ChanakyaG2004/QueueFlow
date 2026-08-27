import json
import os
import random
import re
import signal
import threading
import time
import uuid
from collections import Counter
from contextlib import suppress

import boto3
import psycopg2
from dotenv import load_dotenv


load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
S3_RESULTS_BUCKET = os.getenv("S3_RESULTS_BUCKET")

WORKER_ID = os.getenv("WORKER_ID", str(uuid.uuid4()))
WORKER_NAME = os.getenv("WORKER_NAME", "queueflow-worker")
WORKER_CPU_CAPACITY = int(os.getenv("WORKER_CPU_CAPACITY", "4"))
WORKER_MEMORY_MB = int(os.getenv("WORKER_MEMORY_MB", "8192"))
WORKER_GPU_CAPACITY = int(os.getenv("WORKER_GPU_CAPACITY", "0"))

SQS_VISIBILITY_TIMEOUT_SECONDS = int(os.getenv("SQS_VISIBILITY_TIMEOUT_SECONDS", "120"))
SQS_VISIBILITY_RENEWAL_SECONDS = int(os.getenv("SQS_VISIBILITY_RENEWAL_SECONDS", "30"))
RETRY_BASE_DELAY_SECONDS = int(os.getenv("RETRY_BASE_DELAY_SECONDS", "5"))
RETRY_MAX_DELAY_SECONDS = int(os.getenv("RETRY_MAX_DELAY_SECONDS", "60"))
RESOURCE_MISMATCH_DELAY_SECONDS = int(os.getenv("RESOURCE_MISMATCH_DELAY_SECONDS", "30"))

shutdown_requested = threading.Event()


def log(level, message, **fields):
    print(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "service": "queueflow-worker",
        "message": message,
        "worker_id": WORKER_ID,
        **fields,
    }), flush=True)


def get_database_connection():
    return psycopg2.connect(DATABASE_URL)


def record_event(cursor, job_id, event_type, message=None, metadata=None):
    cursor.execute(
        """
        INSERT INTO job_events (job_id, event_type, message, metadata)
        VALUES (%s, %s, %s, %s)
        """,
        (job_id, event_type, message, json.dumps(metadata) if metadata else None),
    )


def register_worker():
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO workers (
                    id, name, status, cpu_capacity, memory_capacity_mb,
                    gpu_capacity, current_job_id, last_heartbeat
                ) VALUES (%s, %s, 'IDLE', %s, %s, %s, NULL, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    status = 'IDLE',
                    cpu_capacity = EXCLUDED.cpu_capacity,
                    memory_capacity_mb = EXCLUDED.memory_capacity_mb,
                    gpu_capacity = EXCLUDED.gpu_capacity,
                    current_job_id = NULL,
                    last_heartbeat = NOW()
                """,
                (
                    WORKER_ID,
                    WORKER_NAME,
                    WORKER_CPU_CAPACITY,
                    WORKER_MEMORY_MB,
                    WORKER_GPU_CAPACITY,
                ),
            )
        connection.commit()
        log("info", "worker registered", worker_name=WORKER_NAME)
    finally:
        connection.close()


def heartbeat(status="IDLE", current_job_id=None):
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE workers
                SET status = %s, current_job_id = %s, last_heartbeat = NOW()
                WHERE id = %s
                """,
                (status, current_job_id, WORKER_ID),
            )
        connection.commit()
    finally:
        connection.close()


def claim_job(job_id):
    """Atomically grant this worker permission to execute a queued/retrying job."""
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'RUNNING',
                    started_at = COALESCE(started_at, NOW()),
                    attempt_count = attempt_count + 1,
                    worker_id = %s,
                    error = NULL
                WHERE id = %s
                  AND status IN ('QUEUED', 'RETRYING')
                  AND cpu_required <= %s
                  AND memory_required_mb <= %s
                  AND gpu_required <= %s
                RETURNING type, attempt_count, max_attempts,
                          cpu_required, memory_required_mb, gpu_required
                """,
                (
                    WORKER_ID,
                    job_id,
                    WORKER_CPU_CAPACITY,
                    WORKER_MEMORY_MB,
                    WORKER_GPU_CAPACITY,
                ),
            )
            claimed = cursor.fetchone()
            if claimed:
                job_type, attempt, maximum, cpu, memory, gpu = claimed
                record_event(
                    cursor,
                    job_id,
                    "STARTED",
                    metadata={
                        "attempt": attempt,
                        "max_attempts": maximum,
                        "worker_id": WORKER_ID,
                    },
                )
                cursor.execute(
                    """
                    UPDATE workers
                    SET status = 'BUSY', current_job_id = %s, last_heartbeat = NOW()
                    WHERE id = %s
                    """,
                    (job_id, WORKER_ID),
                )
                connection.commit()
                return {
                    "status": "CLAIMED",
                    "type": job_type,
                    "attempt": attempt,
                    "max_attempts": maximum,
                    "cpu": cpu,
                    "memory_mb": memory,
                    "gpu": gpu,
                }

            cursor.execute(
                """
                SELECT status, cpu_required, memory_required_mb, gpu_required
                FROM jobs WHERE id = %s
                """,
                (job_id,),
            )
            existing = cursor.fetchone()
            connection.rollback()
            if existing is None:
                return {"status": "MISSING"}

            status, cpu, memory, gpu = existing
            if status == "RUNNING":
                return {"status": "IN_PROGRESS"}
            if status == "COMPLETED":
                return {"status": "ALREADY_COMPLETED"}
            if status == "FAILED":
                return {"status": "ALREADY_FAILED"}
            if cpu > WORKER_CPU_CAPACITY or memory > WORKER_MEMORY_MB or gpu > WORKER_GPU_CAPACITY:
                cursor.execute(
                    """
                    INSERT INTO job_events (job_id, event_type, message, metadata)
                    SELECT %s, 'RESOURCE_MISMATCH', 'Worker capacity did not satisfy job requirements', %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM job_events
                        WHERE job_id = %s AND event_type = 'RESOURCE_MISMATCH'
                          AND metadata->>'worker_id' = %s
                          AND created_at > NOW() - INTERVAL '5 minutes'
                    )
                    """,
                    (
                        job_id,
                        json.dumps({
                            "worker_id": WORKER_ID,
                            "cpu_required": cpu,
                            "memory_required_mb": memory,
                            "gpu_required": gpu,
                        }),
                        job_id,
                        WORKER_ID,
                    ),
                )
                connection.commit()
                return {
                    "status": "RESOURCE_MISMATCH",
                    "cpu": cpu,
                    "memory_mb": memory,
                    "gpu": gpu,
                }
            return {"status": "NOT_EXECUTABLE", "job_status": status}
    finally:
        connection.close()


def update_progress(job_id, progress):
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs SET progress = %s
                WHERE id = %s AND status = 'RUNNING' AND worker_id = %s
                """,
                (progress, job_id, WORKER_ID),
            )
        connection.commit()
        log("info", "job progress updated", job_id=job_id, progress=progress)
    finally:
        connection.close()


def simulated_compute(job_id):
    for progress in [10, 25, 50, 75, 90]:
        time.sleep(2)
        update_progress(job_id, progress)
    return {"message": "Simulated compute job completed"}


def analyze_text(text):
    words = re.findall(r"\b\w+\b", text.lower())
    sentences = [sentence for sentence in re.split(r"[.!?]+", text) if sentence.strip()]
    return {
        "word_count": len(words),
        "character_count": len(text),
        "sentence_count": len(sentences),
        "top_words": Counter(words).most_common(5),
    }


def save_result_to_s3(s3_client, job_id, result):
    if not S3_RESULTS_BUCKET:
        return None
    key = f"jobs/{job_id}/result.json"
    s3_client.put_object(
        Bucket=S3_RESULTS_BUCKET,
        Key=key,
        Body=json.dumps(result),
        ContentType="application/json",
    )
    return f"s3://{S3_RESULTS_BUCKET}/{key}"


def complete_job(job_id, claim, result, result_location):
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs
                SET status = 'COMPLETED', progress = 100, result = %s,
                    result_location = %s, error = NULL, completed_at = NOW()
                WHERE id = %s AND status = 'RUNNING' AND worker_id = %s
                RETURNING id
                """,
                (json.dumps(result), result_location, job_id, WORKER_ID),
            )
            if not cursor.fetchone():
                connection.rollback()
                return False
            record_event(
                cursor,
                job_id,
                "COMPLETED",
                metadata={
                    "attempt": claim["attempt"],
                    "worker_id": WORKER_ID,
                    "result_location": result_location,
                },
            )
            cursor.execute(
                """
                UPDATE workers SET status = 'IDLE', current_job_id = NULL, last_heartbeat = NOW()
                WHERE id = %s
                """,
                (WORKER_ID,),
            )
        connection.commit()
        return True
    finally:
        connection.close()


def fail_job(job_id, claim, error):
    status = "FAILED" if claim["attempt"] >= claim["max_attempts"] else "RETRYING"
    connection = get_database_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE jobs SET status = %s, error = %s
                WHERE id = %s AND status = 'RUNNING' AND worker_id = %s
                RETURNING id
                """,
                (status, str(error), job_id, WORKER_ID),
            )
            if not cursor.fetchone():
                connection.rollback()
                return "CLAIM_LOST"
            record_event(
                cursor,
                job_id,
                status,
                message=str(error),
                metadata={
                    "attempt": claim["attempt"],
                    "max_attempts": claim["max_attempts"],
                    "worker_id": WORKER_ID,
                },
            )
            cursor.execute(
                """
                UPDATE workers SET status = 'IDLE', current_job_id = NULL, last_heartbeat = NOW()
                WHERE id = %s
                """,
                (WORKER_ID,),
            )
        connection.commit()
        return status
    finally:
        connection.close()


def execute_claimed_job(s3_client, job, claim):
    job_id = job["jobId"]
    try:
        job_type = claim["type"]
        log(
            "info",
            "job execution started",
            job_id=job_id,
            job_type=job_type,
            attempt=claim["attempt"],
            max_attempts=claim["max_attempts"],
        )
        if job_type == "text_analysis":
            result = analyze_text(job.get("text", ""))
        elif job_type == "simulated_compute":
            result = simulated_compute(job_id)
        elif job_type == "always_fail":
            raise RuntimeError("Intentional test failure")
        else:
            raise ValueError(f"Unknown job type: {job_type}")

        location = save_result_to_s3(s3_client, job_id, result)
        if not complete_job(job_id, claim, result, location):
            log("warn", "completion rejected because claim was lost", job_id=job_id)
            return {"success": False, "status": "CLAIM_LOST"}
        log("info", "job completed", job_id=job_id, result_location=location)
        return {"success": True, "status": "COMPLETED"}
    except Exception as error:  # Workload failures are persisted and retried through SQS.
        status = fail_job(job_id, claim, error)
        log(
            "error",
            "job execution failed",
            job_id=job_id,
            status=status,
            attempt=claim["attempt"],
            error=str(error),
        )
        return {"success": False, "status": status}


class VisibilityLease:
    def __init__(self, sqs_client, receipt_handle, job_id):
        self.sqs = sqs_client
        self.receipt_handle = receipt_handle
        self.job_id = job_id
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, name=f"lease-{job_id}", daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=max(1, SQS_VISIBILITY_RENEWAL_SECONDS + 1))

    def _run(self):
        while not self.stopped.wait(SQS_VISIBILITY_RENEWAL_SECONDS):
            try:
                self.sqs.change_message_visibility(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=self.receipt_handle,
                    VisibilityTimeout=SQS_VISIBILITY_TIMEOUT_SECONDS,
                )
                heartbeat("BUSY", self.job_id)
                log("info", "SQS visibility lease renewed", job_id=self.job_id)
            except Exception as error:
                log("error", "visibility lease renewal failed", job_id=self.job_id, error=str(error))


def retry_delay(attempt):
    bounded = min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_DELAY_SECONDS * (2 ** max(0, attempt - 1)))
    return max(1, round(bounded * random.uniform(0.8, 1.2)))


def process_message(sqs_client, s3_client, message):
    receipt = message["ReceiptHandle"]
    try:
        job = json.loads(message["Body"])
        job_id = job["jobId"]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        log("error", "malformed SQS message retained for DLQ", error=str(error))
        return

    receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
    lease = VisibilityLease(sqs_client, receipt, job_id)
    lease.start()
    try:
        claim = claim_job(job_id)
        claim_status = claim["status"]

        if claim_status == "CLAIMED":
            result = execute_claimed_job(s3_client, job, claim)
        else:
            result = {"success": claim_status in {"ALREADY_COMPLETED", "MISSING"}, "status": claim_status}
            log("info", "job message not executed", job_id=job_id, reason=claim_status)
    finally:
        lease.stop()

    status = result["status"]
    if result["success"]:
        sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt)
        log("info", "SQS message acknowledged", job_id=job_id, status=status)
        return

    if status == "RESOURCE_MISMATCH":
        delay = min(RETRY_MAX_DELAY_SECONDS, RESOURCE_MISMATCH_DELAY_SECONDS + retry_delay(receive_count))
    else:
        delay = retry_delay(receive_count)

    sqs_client.change_message_visibility(
        QueueUrl=SQS_QUEUE_URL,
        ReceiptHandle=receipt,
        VisibilityTimeout=delay,
    )
    log("warn", "SQS message retained for retry or DLQ", job_id=job_id, status=status, delay_seconds=delay)


def request_shutdown(signum, _frame):
    shutdown_requested.set()
    log("info", "shutdown requested; polling will stop after current job", signal=signal.Signals(signum).name)


def run_worker():
    if not SQS_QUEUE_URL:
        raise RuntimeError("SQS_QUEUE_URL is not defined")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not defined")

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    while not shutdown_requested.is_set():
        try:
            register_worker()
            break
        except Exception as error:
            log("error", "worker registration failed; retrying", error=str(error))
            shutdown_requested.wait(2)

    if shutdown_requested.is_set():
        log("info", "worker stopped before registration completed")
        return

    sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    log("info", "worker started", worker_name=WORKER_NAME)

    try:
        while not shutdown_requested.is_set():
            try:
                heartbeat("IDLE")
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=SQS_VISIBILITY_TIMEOUT_SECONDS,
                    AttributeNames=["ApproximateReceiveCount"],
                )
                messages = response.get("Messages", [])
                if shutdown_requested.is_set():
                    for message in messages:
                        sqs_client.change_message_visibility(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message["ReceiptHandle"],
                            VisibilityTimeout=0,
                        )
                    break
                for message in messages:
                    process_message(sqs_client, s3_client, message)
            except Exception as error:
                log("error", "worker loop error", error=str(error))
                shutdown_requested.wait(2)
    finally:
        with suppress(Exception):
            heartbeat("OFFLINE")
        log("info", "worker stopped")


if __name__ == "__main__":
    run_worker()
