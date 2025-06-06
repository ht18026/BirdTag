import json
import boto3

sns = boto3.client('sns')

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:590183942241:birdtag-all"


def lambda_handler(event, context):
    """
    event like
    {
        "email": "user@example.com",
        "tags": ["House Crow", "Eagle"]
    }
    """
    email = event.get("email")
    tags = event.get("tags", [])

    if not email or not tags:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Both email and tags are required"})
        }

    filter_policy = {
        "bird_tag": tags
    }

    try:
        response = sns.subscribe(
            TopicArn=SNS_TOPIC_ARN,
            Protocol='email',
            Endpoint=email,
            Attributes={
                'FilterPolicy': json.dumps(filter_policy)
            }
        )
        subscription_arn = response.get('SubscriptionArn', 'PendingConfirmation')
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Subscription created for {email} with tags {tags}. Please check your email to confirm subscription.",
                "SubscriptionArn": subscription_arn
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
