import {
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

export type JobMessage = {
  jobId: string;
  type: string;
  text?: string;
};

export type QueueSender = (message: JobMessage) => Promise<void>;

export async function sendJobToQueue(message: JobMessage) {
  const queueUrl = process.env.SQS_QUEUE_URL;

  if (!queueUrl) {
    throw new Error("SQS_QUEUE_URL is not defined");
  }

  const command = new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(message),
  });

  await sqsClient.send(command);
}
