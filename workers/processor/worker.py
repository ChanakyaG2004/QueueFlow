import os
import json

import boto3
import psycopg2
from dotenv import load_dotenv

from collections import Counter
import re

import time

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")


sqs = boto3.client(
    "sqs",
    region_name=AWS_REGION
)


def get_database_connection():
    return psycopg2.connect(DATABASE_URL)

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
    progress_steps = [10, 25, 50, 75, 90]

    for progress in progress_steps:
        time.sleep(2)

        update_progress(
            cursor,
            connection,
            job_id,
            progress
        )

    return {
        "message": "Simulated compute job completed"
    }

def analyze_text(text):
    words = re.findall(r"\b\w+\b", text.lower())

    word_count = len(words)
    character_count = len(text)

    sentences = re.split(r"[.!?]+", text)
    sentences = [sentence for sentence in sentences if sentence.strip()]

    sentence_count = len(sentences)

    word_frequencies = Counter(words)

    top_words = word_frequencies.most_common(5)

    return {
        "word_count": word_count,
        "character_count": character_count,
        "sentence_count": sentence_count,
        "top_words": top_words
    }

def process_job(job):
    job_id = job["jobId"]
    job_type = job["type"]

    text = job.get("text", "")

    print(f"Processing job {job_id}")
    print(f"Job type: {job_type}")

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        UPDATE jobs
        SET status = %s,
            started_at = NOW()
        WHERE id = %s
        """,
        ("RUNNING", job_id)
    )

    connection.commit()

    print("Job is now RUNNING")

    if job_type == "text_analysis":
        result = analyze_text(text)

    elif job_type == "simulated_compute":
        result = simulated_compute(
            cursor, 
            connection, 
            job_id
        )

    else:
        result = {
        "message": f"Unknown job type: {job_type}"
        }

    print("Analysis result:", result)
    # Actual processing will go here soon.

    cursor.execute(
        """
        UPDATE jobs
        SET status = %s,
            progress = %s,
            result = %s,
            completed_at = NOW()
        WHERE id = %s
        """,
        ("COMPLETED", 100, json.dumps(result), job_id)
    )

    connection.commit()

    cursor.close()
    connection.close()

    print("Job COMPLETED")


def run_worker():

    print("QueueFlow worker started")
    print("Waiting for jobs...")

    while True:

        response = sqs.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20
        )

        messages = response.get("Messages", [])

        if len(messages) == 0:
            continue

        message = messages[0]

        job = json.loads(message["Body"])

        process_job(job)

        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message["ReceiptHandle"]
        )

        print("Message removed from queue")
        print("Waiting for next job...")


if __name__ == "__main__":
    run_worker()