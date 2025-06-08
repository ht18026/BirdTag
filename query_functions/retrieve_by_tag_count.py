import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from urllib.parse import urlparse

# Initialize DynamoDB client and TypeDeserializer
dynamodb_client = boto3.client('dynamodb')
deserializer = TypeDeserializer()

# Environment variables for table and GSI names (set these in your Lambda configuration)
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db') # Replace 'bird-db-Shuyang' with your actual table name
GSI_NAME = os.environ.get('DYNAMODB_GSI_NAME', 'bird_tag-index')       # Replace 'birdTagIndex' with your actual GSI name

# CORS HEADERS
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
}

def lambda_handler(event, context):
    try:
        # 1. Parse the input JSON from the request body
        # The input is expected to be a dictionary with bird species as keys and counts as values
        # Example input: {"crow": 2, "pigeon": 1}
        try:
            request_body = json.loads(event.get('body', '{}'))
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Invalid JSON in request body'})
            }

        if not request_body:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'Request body is empty. Please provide bird tags and counts.'})
            }

        # 2. For each tag in the input, query the GSI
        #    Store media_ids that satisfy the condition for each tag
        #    Also cache media details to avoid re-fetching
        
        sets_of_media_ids_for_each_tag = []
        media_details_cache = {}  # { "media_id_1": {details}, "media_id_2": {details} }

        for bird_species, min_count in request_body.items():
            if not isinstance(min_count, int) or min_count < 0:
                return {
                    'statusCode': 400,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'error': f'Invalid count for {bird_species}. Count must be a non-negative integer.'})
                }

            current_tag_media_ids = set()
            
            # Paginate through GSI query results
            paginator = dynamodb_client.get_paginator('query')
            try:
                page_iterator = paginator.paginate(
                    TableName=TABLE_NAME,
                    IndexName=GSI_NAME,
                    KeyConditionExpression="bird_tag = :tag_val",
                    FilterExpression="#c >= :count_val", # 'count' is a reserved keyword
                    ExpressionAttributeNames={
                        "#c": "count" 
                    },
                    ExpressionAttributeValues={
                        ":tag_val": {"S": bird_species},
                        ":count_val": {"N": str(min_count)}
                    },
                    # Project all necessary attributes. Ensure your GSI is configured to project these.
                    ProjectionExpression="media_id, file_type, full_url, thumb_url" 
                )

                for page in page_iterator:
                    for item_raw in page.get('Items', []):
                        # Deserialize DynamoDB item to Python dict
                        item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                        
                        media_id = item.get('media_id')
                        if media_id:
                            current_tag_media_ids.add(media_id)
                            # Cache details if not already present.
                            # Assumes file_type, full_url, thumb_url are consistent for a given media_id
                            if media_id not in media_details_cache:
                                media_details_cache[media_id] = {
                                    'file_type': item.get('file_type'),
                                    'full_url': item.get('full_url'),
                                    'thumb_url': item.get('thumb_url') # Will be None if not present
                                }
            except Exception as e:
                print(f"Error querying DynamoDB for tag '{bird_species}': {e}")
                return {
                    'statusCode': 500,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({'error': f'Failed to query data for tag: {bird_species}. Details: {str(e)}'})
                }

            if not current_tag_media_ids:
                return {
                    'statusCode': 200,
                    'headers': CORS_HEADERS,
                    'body': json.dumps({"links": []})
                }
            sets_of_media_ids_for_each_tag.append(current_tag_media_ids)

        # 3. Perform intersection of media_ids
        if not sets_of_media_ids_for_each_tag:
            return {
                'statusCode': 200,
                'headers': CORS_HEADERS,
                'body': json.dumps({"links": []})
            }

        intersected_media_ids = sets_of_media_ids_for_each_tag[0].copy()
        for i in range(1, len(sets_of_media_ids_for_each_tag)):
            intersected_media_ids.intersection_update(sets_of_media_ids_for_each_tag[i])

        # 4. Construct the response with URLs
        # 4. Construct the response with both thumb and full-size URLs
        presigned_expiration = int(os.environ.get('PRESIGNED_URL_EXPIRATION', '3600'))
        results = []

        for media_id in intersected_media_ids:
            details = media_details_cache.get(media_id)
            if not details:
                print(f"Warning: Details for media_id {media_id} not found in cache. Skipping.")
                continue

            thumb_url = details.get('thumb_url')
            full_url = details.get('full_url')
            file_type = details.get('file_type')

            presigned_thumb = generate_presigned_url(thumb_url, presigned_expiration) if thumb_url else None
            presigned_full = generate_presigned_url(full_url, presigned_expiration) if full_url else None

            results.append({
                "media_id": media_id,
                "file_type": file_type,
                "thumb_url": presigned_thumb,
                "full_url": presigned_full
            })

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                "results": results,
                "total_matches": len(results),
                "presigned_expiration": presigned_expiration
            })
        }

    except Exception as e:
        print(f"Unhandled error in lambda_handler: {e}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Internal server error. Check Lambda logs for details.'})
        }

# **NEW: Add presigned URL functions**
def generate_presigned_url(s3_url, expiration=3600):
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
        
        return presigned_url
    except ClientError as e:
        print(f"Error generating presigned URL for {s3_url}: {e}")
        return s3_url
    except Exception as e:
        print(f"Unexpected error generating presigned URL for {s3_url}: {e}")
        return s3_url

def parse_s3_url(s3_url):
    if not s3_url:
        return None, None
    try:
        if s3_url.startswith('s3://'):
            parts = s3_url[5:].split('/', 1)
            return parts[0], parts[1] if len(parts) > 1 else ''
        elif 'amazonaws.com' in s3_url:
            parsed = urlparse(s3_url)
            if '.s3.' in parsed.hostname:
                return parsed.hostname.split('.s3.')[0], parsed.path.lstrip('/')
            elif parsed.hostname.startswith('s3.'):
                path_parts = parsed.path.lstrip('/').split('/', 1)
                return path_parts[0], path_parts[1] if len(path_parts) > 1 else ''
    except Exception as e:
        print(f"Error parsing S3 URL {s3_url}: {e}")
    return None, None

def generate_presigned_urls_batch(urls, expiration=3600):
    if not urls:
        return []
    
    presigned_urls = []
    print(f"Generating presigned URLs for {len(urls)} files")
    for url in urls:
        presigned_url = generate_presigned_url(url, expiration)
        presigned_urls.append(presigned_url)
    return presigned_urls