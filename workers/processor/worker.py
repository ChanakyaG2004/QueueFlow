import os
import json
import time
import re
import uuid
from collections import Counter

import boto3
import psycopg2
from dotenv import load_dotenv


load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
S3_RESULTS_BUCKET = os.getenv("S3_RESULTS_BUCKET")

WORKER_ID = str(uuid.uuid4())
WORKER_NAME = os.getenv("WORKER_NAME", "queueflow-worker")
WORKER_CPU_CAPACITY = int(os.getenv("WORKER_CPU_CAPACITY", "4"))
WORKER_MEMORY_MB = int(os.getenv("WORKER_MEMORY_MB", "8192"))
WORKER_GPU_CAPACITY = int(os.getenv("WORKER_GPU_CAPACITY", "0"))

sqs = boto3.client("sqs", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


def get_database_connection():
    return psycopg2.connect(DATABASE_URL)


def record_event(
    cursor,
    connection,
    job_id,
    event_type,
    message=None,
    metadata=None
):
    cursor.execute(
        """
        INSERT INTO job_events (
            job_id,
            event_type,
            message,
            metadata
        )
        VALUES (%s, %s, %s, %s)
        """,
        (
            job_id,
            event_type,
            message,
            json.dumps(metadata) if metadata else None
        )
    )
    connection.commit()


def register_worker():
    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO workers (
            id,
            name,
            status,
            cpu_capacity,
            memory_capacity_mb,
            gpu_capacity,
            last_heartbeat
        )
        VALUES (%s, %s, 'IDLE', %s, %s, %s, NOW())
        ON CONFLICT (id)
        DO UPDATE SET
            status = 'IDLE',
            last_heartbeat = NOW()
        """,
        (
            WORKER_ID,
            WORKER_NAME,
            WORKER_CPU_CAPACITY,
            WORKER_MEMORY_MB,
            WORKER_GPU_CAPACITY
        )
    )

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Worker registered: {WORKER_NAME} ({WORKER_ID})")


def heartbeat(status="IDLE", current_job_id=None):
    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE workers
        SET status = %s,
            current_job_id = %s,
            last_heartbeat = NOW()
        WHERE id = %s
        """,
        (status, current_job_id, WORKER_ID)
    )

    connection.commit()
    cursor.close()
    connection.close()


def update_progress(cursor, connection, job_id, progress):
    cursor.execute(
        """
        UPDATE jobs
        SET progress = %s
        WHERE id = %s
        """,
        (progress, job_id)
    )

    connection.commit()
    print(f"Job progress: {progress}%")


def simulated_compute(cursor, connection, job_id):
    for progress in [10, 25, 50, 75, 90]:
        time.sleep(2)
        update_progress(cursor, connection, job_id, progress)

    return {
        "message": "Simulated compute job completed"
    }


def analyze_text(text):
    words = re.findall(r"\b\w+\b", text.lower())
    sentences = [
        sentence
        for sentence in re.split(r"[.!?]+", text)
        if sentence.strip()
    ]

    return {
        "word_count": len(words),
        "character_count": len(text),
        "sentence_count": len(sentences),
        "top_words": Counter(words).most_common(5)
    }


def save_result_to_s3(job_id, result):
    if not S3_RESULTS_BUCKET:
        return None

    key = f"jobs/{job_id}/result.json"

    s3.put_object(
        Bucket=S3_RESULTS_BUCKET,
        Key=key,
        Body=json.dumps(result),
        ContentType="application/json"
    )

    return f"s3://{S3_RESULTS_BUCKET}/{key}"


def process_job(job):
    job_id = job["jobId"]
    job_type = job["type"]
    text = job.get("text", "")

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                cpu_required,
                memory_required_mb,
                gpu_required
            FROM jobs
            WHERE id = %s
            """,
            (job_id,)
        )

        requirements = cursor.fetchone()

        if requirements is None:
            raise RuntimeError(f"Job {job_id} does not exist")

        cpu_required, memory_required_mb, gpu_required = requirements

        if (
            cpu_required > WORKER_CPU_CAPACITY
            or memory_required_mb > WORKER_MEMORY_MB
            or gpu_required > WORKER_GPU_CAPACITY
        ):
            print("Worker does not have enough resources for job")

            record_event(
                cursor,
                connection,
                job_id,
                "RESOURCE_MISMATCH",
                metadata={
                    "worker_id": WORKER_ID,
                    "cpu_required": cpu_required,
                    "memory_required_mb": memory_required_mb,
                    "gpu_required": gpu_required
                }
            )

            return {
                "success": False,
                "status": "RESOURCE_MISMATCH"
            }

        cursor.execute(
            """
            UPDATE jobs
            SET status = 'RUNNING',
                started_at = COALESCE(started_at, NOW()),
                attempt_count = attempt_count + 1,
                worker_id = %s
            WHERE id = %s
            RETURNING attempt_count, max_attempts
            """,
            (WORKER_ID, job_id)
        )

        attempt_count, max_attempts = cursor.fetchone()
        connection.commit()

        heartbeat("BUSY", job_id)

        print(f"Processing job {job_id}")
        print(f"Job type: {job_type}")
        print(f"Attempt {attempt_count}/{max_attempts}")

        record_event(
            cursor,
            connection,
            job_id,
            "STARTED",
            metadata={
                "attempt": attempt_count,
                "max_attempts": max_attempts,
                "worker_id": WORKER_ID
            }
        )

        if job_type == "text_analysis":
            result = analyze_text(text)

        elif job_type == "simulated_compute":
            result = simulated_compute(
                cursor,
                connection,
                job_id
            )

        elif job_type == "always_fail":
            raise RuntimeError("Intentional test failure")

        else:
            raise ValueError(f"Unknown job type: {job_type}")

        result_location = save_result_to_s3(job_id, result)

        cursor.execute(
            """
            UPDATE jobs
            SET status = 'COMPLETED',
                progress = 100,
                result = %s,
                result_location = %s,
                error = NULL,
                completed_at = NOW()
            WHERE id = %s
            """,
            (
                json.dumps(result),
                result_location,
                job_id
            )
        )

        connection.commit()

        record_event(
            cursor,
            connection,
            job_id,
            "COMPLETED",
            metadata={
                "attempt": attempt_count,
                "worker_id": WORKER_ID,
                "result_location": result_location
            }
        )

        print("Job COMPLETED")

        return {
            "success": True,
            "status": "COMPLETED"
        }

    except Exception as error:
        connection.rollback()

        cursor.execute(
            """
            SELECT attempt_count, max_attempts
            FROM jobs
            WHERE id = %s
            """,
            (job_id,)
        )

        row = cursor.fetchone()

        if row is None:
            return {
                "success": False,
                "status": "UNKNOWN"
            }

        attempt_count, max_attempts = row

        status = (
            "FAILED"
            if attempt_count >= max_attempts
            else "RETRYING"
        )

        cursor.execute(
            """
            UPDATE jobs
            SET status = %s,
                error = %s
            WHERE id = %s
            """,
            (status, str(error), job_id)
        )

        connection.commit()

        record_event(
            cursor,
            connection,
            job_id,
            status,
            message=str(error),
            metadata={
                "attempt": attempt_count,
                "max_attempts": max_attempts,
                "worker_id": WORKER_ID
            }
        )

        print(f"Job failed: {error}")
        print(f"Job status: {status}")

        return {
            "success": False,
            "status": status
        }

    finally:
        heartbeat("IDLE")
        cursor.close()
        connection.close()


def run_worker():
    register_worker()

    print("QueueFlow worker started")
    print("Waiting for jobs...")

    while True:
        try:
            heartbeat("IDLE")

            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )

            messages = response.get("Messages", [])

            if not messages:
                continue

            message = messages[0]
            job = json.loads(message["Body"])

            result = process_job(job)

            if result["success"]:
                sqs.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message["ReceiptHandle"]
                )

                print("Message removed from queue")

            elif result["status"] == "RESOURCE_MISMATCH":
                sqs.change_message_visibility(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message["ReceiptHandle"],
                    VisibilityTimeout=10
                )

                print("Job requires a different worker")

            else:
                print("Message retained for SQS retry/DLQ")

        except KeyboardInterrupt:
            heartbeat("OFFLINE")
            print("\nQueueFlow worker stopped")
            break

        except Exception as error:
            print(f"Worker loop error: {error}")
            time.sleep(2)


if __name__ == "__main__":
    run_worker()