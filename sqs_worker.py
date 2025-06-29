import boto3
import os
import json
import asyncio
from dotenv import load_dotenv

load_dotenv()

sqs = boto3.client(
    'sqs',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)
SQS_URL = os.getenv("SQS_URL")

async def poll_sqs():
    while True:
        response = sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        messages = response.get("Messages", [])
        for message in messages:
            body = json.loads(message["Body"])
            print("Received SQS message:")
            print(json.dumps(body, indent=2))

            # Delete after processing
            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=message["ReceiptHandle"]
            )

        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(poll_sqs())
