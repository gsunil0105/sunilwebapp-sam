import json
import os
import boto3
import urllib.parse
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
sns = boto3.client('sns')
sqs = boto3.client('sqs')

# Environment variables (from template.yaml)
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']

def lambda_handler(event, context):
    """
    Lambda handler triggered by S3 object-created events.
    Publishes a message to SNS (for email) and SQS (for downstream consumers).
    """
    logger.info("Received event from codebuild: %s", json.dumps(event))

    records = event.get('Records', [])
    msgs = []

    for r in records:
        # Only process S3 events
        if r.get('eventSource') == 'aws:s3':
            bucket = r['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(r['s3']['object']['key'])
            size = r['s3']['object'].get('size')

            msg = {
                "type": "image_upload",
                "bucket": bucket,
                "key": key,
                "size": size,
                "eventTime": r.get('eventTime'),
            }
            msgs.append(msg)

    if not msgs:
        logger.warning("No S3 records found in event.")
        return {"statusCode": 200, "body": "No S3 records to process."}

    # --- Publish to SNS (email notifications) ---
    try:
        subject = "New image uploaded"
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(msgs, indent=2),
            Subject=subject
        )
        logger.info("Published %d messages to SNS topic %s", len(msgs), SNS_TOPIC_ARN)
    except Exception as e:
        logger.error("Failed to publish to SNS: %s", e)

    # --- Send to SQS (for further processing) ---
    for m in msgs:
        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(m)
            )
            logger.info("Sent message to SQS queue %s: %s", SQS_QUEUE_URL, m)
        except Exception as e:
            logger.error("Failed to send message to SQS: %s", e)

    return {"statusCode": 200, "body": f"Processed {len(msgs)} image uploads."}
