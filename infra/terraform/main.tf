terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_sqs_queue" "jobs_dlq" {
  name = "queueflow-jobs-dlq"
}

resource "aws_sqs_queue" "jobs" {
  name                       = "queueflow-jobs"
  visibility_timeout_seconds = 30
  receive_wait_time_seconds  = 20
}

resource "aws_sqs_queue_redrive_policy" "jobs" {
  queue_url = aws_sqs_queue.jobs.id

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.jobs_dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_s3_bucket" "results" {
  bucket = "queueflow-results-819379976838-us-east-1"
}

output "jobs_queue_url" {
  value = aws_sqs_queue.jobs.url
}

output "dlq_url" {
  value = aws_sqs_queue.jobs_dlq.url
}

output "results_bucket" {
  value = aws_s3_bucket.results.bucket
}