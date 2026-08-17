import {
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

const sqsClient = new SQSClient({
  region: process.env.AWS_REGION || "us-east-1",
});

export async function sendJobToQueue(jobId: string, type: string, text?:string) {
  const queueUrl = process.env.SQS_QUEUE_URL;

  if (!queueUrl) {
    throw new Error("SQS_QUEUE_URL is not defined");
  }

  const command = new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify({
      jobId,
      type,
      text,
    }),
  });

  await sqsClient.send(command);
}