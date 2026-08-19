import time
import requests


class QueueFlowClient:
    def __init__(
        self,
        base_url="http://localhost:3001",
        api_key="queueflow-demo-key"
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }

    def submit(
        self,
        job_type,
        text=None,
        cpu=1,
        memory_mb=256,
        gpu=0,
        priority=0
    ):
        payload = {
            "type": job_type,
            "cpu": cpu,
            "memoryMb": memory_mb,
            "gpu": gpu,
            "priority": priority
        }

        if text is not None:
            payload["text"] = text

        response = requests.post(
            f"{self.base_url}/jobs",
            headers=self.headers,
            json=payload,
            timeout=10
        )

        response.raise_for_status()
        return response.json()

    def get(self, job_id):
        response = requests.get(
            f"{self.base_url}/jobs/{job_id}",
            headers=self.headers,
            timeout=10
        )

        response.raise_for_status()
        return response.json()

    def events(self, job_id):
        response = requests.get(
            f"{self.base_url}/jobs/{job_id}/events",
            headers=self.headers,
            timeout=10
        )

        response.raise_for_status()
        return response.json()

    def wait(self, job_id, interval=1):
        while True:
            job = self.get(job_id)

            if job["status"] in {
                "COMPLETED",
                "FAILED",
                "SUBMISSION_FAILED"
            }:
                return job

            time.sleep(interval)