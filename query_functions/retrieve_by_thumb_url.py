import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from urllib.parse import urlparse

# Initialize DynamoDB client and TypeDeserializer
dynamodb_client = boto3.client('dynamodb')
deserializer = TypeDeserializer()

# Environment variables for table and GSI names
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db')
THUMB_URL_GSI_NAME = os.environ.get('DYNAMODB_THUMB_GSI_NAME', 'thumb_url-index') # GSI for thumbnail URL lookup

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}") 

    # CORS headers for API Gateway
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
    }
    
    # Handle CORS preflight request
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'message': 'CORS preflight'})
        }
    
    try:
        # 1. The 'event' is already the parsed JSON object from the request body
        # The input is expected to be a dictionary with a 'thumbnail_url' key
    # {
#     "thumbnail_url": "s3://your-bucket-name/path/to/your/thumbnail.jpg"
#     }
        try:
            # parse the JSON body from the event
            if isinstance(event.get('body'), str):
                payload = json.loads(event['body'])
            else:
                # if the body is already a dict, use it directly
                payload = event
        except (json.JSONDecodeError, TypeError) as e:
            print(f"JSON parsing error: {e}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Invalid JSON in request body.'})
            }
        
        print(f"Parsed payload: {payload}")
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

        # **NEW: Generate presigned URL for the full image**
        presigned_expiration = int(os.environ.get('PRESIGNED_URL_EXPIRATION', '3600'))
        presigned_full_url = generate_presigned_url(full_image_url, presigned_expiration)

        return {
            'statusCode': 200,
            'body': json.dumps({
                "full_image_url": presigned_full_url,  # Return presigned URL
                "presigned_expiration": presigned_expiration
            })
        }

    except Exception as e:
        print(f"Unhandled error in lambda_handler: {e}") 
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error. Check Lambda logs for details.'})
        }
    
# **NEW: Add presigned URL functions**
def generate_presigned_url(s3_url, expiration=3600):
    """Generate a presigned URL for an S3 object"""
    try:
        s3_client = boto3.client('s3')
        bucket_name, object_key = parse_s3_url(s3_url)
        
        if not bucket_name or not object_key:
            print(f"Warning: Could not parse S3 URL: {s3_url}")
            return s3_url
        
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
        
        print(f"Generated presigned URL for s3://{bucket_name}/{object_key}")
        return presigned_url
        
    except ClientError as e:
        print(f"Error generating presigned URL for {s3_url}: {e}")
        return s3_url
    except Exception as e:
        print(f"Unexpected error generating presigned URL for {s3_url}: {e}")
        return s3_url

def parse_s3_url(s3_url):
    """Parse S3 URL to extract bucket name and object key"""
    if not s3_url:
        return None, None
    
    try:
        if s3_url.startswith('s3://'):
            parts = s3_url[5:].split('/', 1)
            bucket_name = parts[0]
            object_key = parts[1] if len(parts) > 1 else ''
            return bucket_name, object_key
        
        elif 'amazonaws.com' in s3_url:
            parsed = urlparse(s3_url)
            
            if '.s3.' in parsed.hostname:
                bucket_name = parsed.hostname.split('.s3.')[0]
                object_key = parsed.path.lstrip('/')
                return bucket_name, object_key
            elif parsed.hostname.startswith('s3.'):
                path_parts = parsed.path.lstrip('/').split('/', 1)
                bucket_name = path_parts[0] if path_parts else ''
                object_key = path_parts[1] if len(path_parts) > 1 else ''
                return bucket_name, object_key
        
        return None, None
            
    except Exception as e:
        print(f"Error parsing S3 URL {s3_url}: {e}")
        return None, None