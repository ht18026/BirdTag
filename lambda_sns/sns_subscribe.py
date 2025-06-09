import json
import boto3

sns = boto3.client('sns')

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:767397663612:birdtag-all"

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'POST,OPTIONS'
}

def lambda_handler(event, context):
    # Handle preflight OPTIONS request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "CORS preflight success"})
        }

    try:
        body = json.loads(event.get("body", "{}"))
        email = body.get("email")
        tags = body.get("tags", [])

        if not email or not tags:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "Both email and tags are required"})
            }
        #remove existing subscriptions
        existing_subs = sns.list_subscriptions_by_topic(TopicArn=SNS_TOPIC_ARN)

        for sub in existing_subs.get('Subscriptions', []):
            if sub['Protocol'] == 'email' and sub['Endpoint'] == email:
                sns.unsubscribe(SubscriptionArn=sub['SubscriptionArn'])
                break  # Only one should exist
        filter_policy = {
            "bird_tag": tags
        }

        response = sns.subscribe(
            TopicArn=SNS_TOPIC_ARN,
            Protocol='email',
            Endpoint=email,
            Attributes={
                'FilterPolicy': json.dumps(filter_policy)
            }
        )

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": f"Subscription created for {email} with tags {tags}. Please check your email to confirm subscription.",
                "SubscriptionArn": response.get("SubscriptionArn", "PendingConfirmation")
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)})
        }