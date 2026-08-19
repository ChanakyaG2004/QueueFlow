from queueflow import QueueFlowClient


client = QueueFlowClient()

job = client.submit(
    "text_analysis",
    text="QueueFlow SDK submission works."
)

print("Submitted:", job["id"])

finished = client.wait(job["id"])

print("Status:", finished["status"])
print("Result:", finished["result"])
print("S3:", finished["result_location"])