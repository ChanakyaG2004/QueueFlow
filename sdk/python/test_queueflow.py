import unittest
from unittest.mock import Mock, patch

import requests

from queueflow import QueueFlowClient


class ListingTests(unittest.TestCase):
    def test_listing_endpoints_preserve_authentication_and_response(self):
        client = QueueFlowClient("https://example.test/", "test-key")
        for method, endpoint in ((client.list_jobs, "jobs"), (client.list_workers, "workers")):
            with self.subTest(endpoint=endpoint), patch("queueflow.requests.get") as get:
                payload = [{"id": "example"}]
                get.return_value.json.return_value = payload
                self.assertEqual(method(), payload)
                get.assert_called_once_with(
                    f"https://example.test/{endpoint}",
                    headers={"x-api-key": "test-key", "Content-Type": "application/json"},
                    timeout=10,
                )
                get.return_value.raise_for_status.assert_called_once_with()

    def test_listing_errors_propagate_without_parsing_response(self):
        client = QueueFlowClient()
        for method in (client.list_jobs, client.list_workers):
            with self.subTest(method=method.__name__), patch("queueflow.requests.get") as get:
                get.return_value = Mock()
                get.return_value.raise_for_status.side_effect = requests.HTTPError("401")
                with self.assertRaises(requests.HTTPError):
                    method()
                get.return_value.json.assert_not_called()


if __name__ == "__main__":
    unittest.main()
