# QueueFlow Python SDK

Install `requests` and run from this directory, or add it to `PYTHONPATH`:

```python
from queueflow import QueueFlowClient

client = QueueFlowClient(base_url="http://localhost:3001", api_key="your-api-key")
jobs = client.list_jobs()
workers = client.list_workers()
```

`list_jobs()` returns the jobs visible to your tenant. `list_workers()` returns
registered workers with their health and capacity information. Both helpers
return the API's decoded JSON response and propagate HTTP errors.

Run the SDK tests from the repository root:

```sh
python -m unittest discover -s sdk/python -p 'test_*.py'
```
