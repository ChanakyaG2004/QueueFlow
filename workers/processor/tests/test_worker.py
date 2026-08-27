import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import worker  # noqa: E402


class FakeCursor:
    def __init__(self, claim=None, existing=None, transition=True):
        self.claim = claim
        self.existing = existing
        self.transition = transition
        self.next_row = None
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.statements.append((normalized, params))
        if "UPDATE jobs SET status = 'RUNNING'" in normalized:
            self.next_row = self.claim
        elif "SELECT status, cpu_required" in normalized:
            self.next_row = self.existing
        elif "UPDATE jobs SET status = 'COMPLETED'" in normalized:
            self.next_row = ("job-1",) if self.transition else None
        elif "UPDATE jobs SET status = %s" in normalized:
            self.next_row = ("job-1",) if self.transition else None

    def fetchone(self):
        value = self.next_row
        self.next_row = None
        return value


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


class WorkerStateTests(unittest.TestCase):
    def test_successful_claim_is_conditional_and_records_started_atomically(self):
        cursor = FakeCursor(claim=("text_analysis", 1, 3, 1, 256, 0))
        connection = FakeConnection(cursor)
        with patch.object(worker, "get_database_connection", return_value=connection):
            result = worker.claim_job("job-1")

        self.assertEqual(result["status"], "CLAIMED")
        claim_sql = cursor.statements[0][0]
        self.assertIn("status IN ('QUEUED', 'RETRYING')", claim_sql)
        self.assertIn("cpu_required <= %s", claim_sql)
        self.assertTrue(any("INSERT INTO job_events" in sql for sql, _ in cursor.statements))
        self.assertEqual(connection.commits, 1)

    def test_duplicate_running_delivery_is_not_claimed(self):
        cursor = FakeCursor(existing=("RUNNING", 1, 256, 0))
        with patch.object(worker, "get_database_connection", return_value=FakeConnection(cursor)):
            self.assertEqual(worker.claim_job("job-1")["status"], "IN_PROGRESS")

    def test_already_completed_delivery_is_safe_to_acknowledge(self):
        cursor = FakeCursor(existing=("COMPLETED", 1, 256, 0))
        with patch.object(worker, "get_database_connection", return_value=FakeConnection(cursor)):
            self.assertEqual(worker.claim_job("job-1")["status"], "ALREADY_COMPLETED")

    def test_resource_mismatch_does_not_increment_attempt(self):
        cursor = FakeCursor(existing=("QUEUED", 99, 256, 0))
        with patch.object(worker, "get_database_connection", return_value=FakeConnection(cursor)):
            result = worker.claim_job("job-1")
        self.assertEqual(result["status"], "RESOURCE_MISMATCH")
        self.assertEqual(result["cpu"], 99)

    def test_retry_and_final_failure_follow_max_attempts(self):
        for attempt, expected in [(1, "RETRYING"), (3, "FAILED")]:
            cursor = FakeCursor()
            with patch.object(worker, "get_database_connection", return_value=FakeConnection(cursor)):
                status = worker.fail_job("job-1", {"attempt": attempt, "max_attempts": 3}, RuntimeError("boom"))
            self.assertEqual(status, expected)
            self.assertTrue(any("INSERT INTO job_events" in sql for sql, _ in cursor.statements))

    def test_completion_and_event_are_one_database_commit(self):
        cursor = FakeCursor()
        connection = FakeConnection(cursor)
        with patch.object(worker, "get_database_connection", return_value=connection):
            completed = worker.complete_job(
                "job-1", {"attempt": 1}, {"ok": True}, "s3://bucket/jobs/job-1/result.json"
            )
        self.assertTrue(completed)
        self.assertEqual(connection.commits, 1)
        self.assertTrue(any("INSERT INTO job_events" in sql for sql, _ in cursor.statements))

    def test_completed_duplicate_message_is_deleted_without_execution(self):
        sqs = Mock()
        message = {
            "Body": '{"jobId":"job-1","type":"text_analysis","text":"hello"}',
            "ReceiptHandle": "receipt",
            "Attributes": {"ApproximateReceiveCount": "2"},
        }
        with patch.object(worker, "claim_job", return_value={"status": "ALREADY_COMPLETED"}), \
             patch.object(worker.VisibilityLease, "start"), \
             patch.object(worker.VisibilityLease, "stop"):
            worker.process_message(sqs, Mock(), message)
        sqs.delete_message.assert_called_once()


if __name__ == "__main__":
    unittest.main()
