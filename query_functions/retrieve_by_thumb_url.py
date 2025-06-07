import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer

# Initialize DynamoDB client and TypeDeserializer
dynamodb_client = boto3.client('dynamodb')
deserializer = TypeDeserializer()

# Environment variables for table and GSI names
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db-Shuyang')
THUMB_URL_GSI_NAME = os.environ.get('DYNAMODB_THUMB_GSI_NAME', 'thumb_url-index') # GSI for thumbnail URL lookup

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}") 

    try:
        # 1. The 'event' is already the parsed JSON object from the request body
        # The input is expected to be a dictionary with a 'thumbnail_url' key
    # {
#     "thumbnail_url": "s3://your-bucket-name/path/to/your/thumbnail.jpg"
#     }
        payload = event 
        
        print(f"Using event directly as payload: {payload}")
        print(f"Type of payload: {type(payload)}")

        input_thumb_url = payload.get('thumbnail_url')
        print(f"Value of input_thumb_url: {input_thumb_url}") 
        print(f"Type of input_thumb_url: {type(input_thumb_url)}")


        # Validate that thumbnail_url is a non-empty string
        if not isinstance(input_thumb_url, str) or not input_thumb_url.strip():
            print("Validation failed for input_thumb_url") 
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body must contain a non-empty "thumbnail_url" string.'})
            }
        
        # Strip any leading/trailing whitespace just in case
        input_thumb_url = input_thumb_url.strip()

        # 2. Query the GSI using the thumbnail URL
        found_items = []
        try:
            query_params = {
                'TableName': TABLE_NAME,
                'IndexName': THUMB_URL_GSI_NAME,
                'KeyConditionExpression': "thumb_url = :thumb_val", # Querying by the GSI's partition key
                'ExpressionAttributeValues': {
                    ":thumb_val": {"S": input_thumb_url}
                },
                'ProjectionExpression': "full_url", # Ensure your GSI projects 'full_url'
                'Limit': 1 # We expect one primary item for a given thumbnail URL
            }
            
            result = dynamodb_client.query(**query_params)
            
            for item_raw in result.get('Items', []):
                # Deserialize DynamoDB item to Python dict
                item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                found_items.append(item)
        
        except Exception as e:
            print(f"Error querying DynamoDB with thumbnail URL '{input_thumb_url}': {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': f'Failed to query data using thumbnail URL. Details: {str(e)}'})
            }

        # 3. Process the result
        if not found_items:
            return {
                'statusCode': 404, # Not Found
                'body': json.dumps({'error': 'No matching record found for the provided thumbnail URL.'})
            }

        # Since Limit:1 is used, we expect at most one item.
        first_match = found_items[0]
        full_image_url = first_match.get('full_url')

        if not full_image_url:
            # This case implies the item was found by thumb_url, but it doesn't have a full_url.
            # This could indicate data inconsistency.
            print(f"Warning: Record found for thumbnail '{input_thumb_url}' but 'full_url' attribute is missing or empty.")
            return {
                'statusCode': 404, # Or 500 if this is considered a server-side data error
                'body': json.dumps({'error': 'Matching record found, but the full-size image URL is missing.'})
            }

        # Return the full-size image URL
        return {
            'statusCode': 200,
            'body': json.dumps({"full_image_url": full_image_url})
        }

    except Exception as e:
        print(f"Unhandled error in lambda_handler: {e}") 
        import traceback
        traceback.print_exc() # Log detailed exception to CloudWatch
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error. Check Lambda logs for details.'})
        }