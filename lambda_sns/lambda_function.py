import json
import boto3

sns = boto3.client('sns')

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:590183942241:birdtag-all"


def lambda_handler(event, context):
    """
    event like
    {
        "message": "Processing complete",
        "tags": {
            "House Crow": 1,
            "Eagle": 2
        }
    }
    """
    print("Received event:")
    print(json.dumps(event, indent=2))
    tags = event.get("responsePayload", {}).get("tags", {}) # due to aws will wrap the response with other messages


    if not tags:
        return {"statusCode": 400, "body": json.dumps({"error": "No tags provided"})}

    results = []
    for tag, count in tags.items():
        message = (
            f"Detected bird: {tag}\n"
            f"Count: {count}\n"
        )

        try:
            response = sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"Bird detected: {tag}",
                Message=message,
                MessageAttributes={
                    'bird_tag': {
                        'DataType': 'String',
                        'StringValue': tag
                    }
                }
            )
            print(f"Published SNS message for {tag}: {response}")
            results.append({"tag": tag, "MessageId": response.get("MessageId")})
        except Exception as e:
            print(f"Error publishing SNS message for {tag}: {e}")
            results.append({"tag": tag, "error": str(e)})

    return {
        "statusCode": 200,
        "body": json.dumps({"publish_results": results})
    }
