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
GSI_NAME = os.environ.get('DYNAMODB_GSI_NAME', 'bird_tag-index')

# CORS headers wrapper
def add_cors_headers(response):
    response['headers'] = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'OPTIONS,POST'
    }
    return response

def lambda_handler(event, context):
    try:
        try:
            request_body = json.loads(event.get('body', '[]'))
        except json.JSONDecodeError:
            return add_cors_headers({
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in request body'})
            })

        if not isinstance(request_body, list):
            return add_cors_headers({
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body must be a list of bird tags.'})
            })

        if not request_body:
            return add_cors_headers({
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body is empty. Please provide bird tags.'})
            })

        sets_of_media_ids_for_each_tag = []
        media_details_cache = {}

        for bird_species in request_body:
            if not isinstance(bird_species, str) or not bird_species.strip():
                print(f"Warning: Invalid or empty bird species tag found: '{bird_species}'. Skipping.")
                continue

            current_tag_media_ids = set()

            paginator = dynamodb_client.get_paginator('query')
            try:
                page_iterator = paginator.paginate(
                    TableName=TABLE_NAME,
                    IndexName=GSI_NAME,
                    KeyConditionExpression="bird_tag = :tag_val",
                    ExpressionAttributeValues={
                        ":tag_val": {"S": bird_species}
                    },
                    ProjectionExpression="media_id, file_type, full_url, thumb_url"
                )

                for page in page_iterator:
                    for item_raw in page.get('Items', []):
                        item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                        media_id = item.get('media_id')
                        if media_id:
                            current_tag_media_ids.add(media_id)
                            if media_id not in media_details_cache:
                                media_details_cache[media_id] = {
                                    'file_type': item.get('file_type'),
                                    'full_url': item.get('full_url'),
                                    'thumb_url': item.get('thumb_url')
                                }
            except Exception as e:
                print(f"Error querying DynamoDB for tag '{bird_species}': {e}")
                return add_cors_headers({
                    'statusCode': 500,
                    'body': json.dumps({'error': f'Failed to query data for tag: {bird_species}. Details: {str(e)}'})
                })

            if not current_tag_media_ids:
                return add_cors_headers({
                    'statusCode': 200,
                    'body': json.dumps({"results": []})
                })

            sets_of_media_ids_for_each_tag.append(current_tag_media_ids)

        if not sets_of_media_ids_for_each_tag:
            return add_cors_headers({
                'statusCode': 200,
                'body': json.dumps({"results": []})
            })

        intersected_media_ids = sets_of_media_ids_for_each_tag[0].copy()
        for i in range(1, len(sets_of_media_ids_for_each_tag)):
            intersected_media_ids.intersection_update(sets_of_media_ids_for_each_tag[i])

        results = []
        for media_id in intersected_media_ids:
            details = media_details_cache.get(media_id)
            if not details:
                continue

            file_type = details.get('file_type')
            thumb_url = details.get('thumb_url')
            full_url = details.get('full_url')

            presigned_thumb = generate_presigned_url(thumb_url) if thumb_url else None
            presigned_full = generate_presigned_url(full_url) if full_url else None

            results.append({
                "file_type": file_type,
                "full_url": presigned_full,
                "thumb_url": presigned_thumb
            })

        presigned_expiration = int(os.environ.get('PRESIGNED_URL_EXPIRATION', '3600'))

        return add_cors_headers({
            'statusCode': 200,
            'body': json.dumps({
                "results": results,
                "total_matches": len(results),
                "presigned_expiration": presigned_expiration
            })
        })

    except Exception as e:
        print(f"Unhandled error in lambda_handler: {e}")
        import traceback
        traceback.print_exc()
        return add_cors_headers({
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error. Check Lambda logs for details.'})
        })

def generate_presigned_url(s3_url, expiration=3600):
    try:
        s3_client = boto3.client('s3')
        bucket_name, object_key = parse_s3_url(s3_url)
        if not bucket_name or not object_key:
            print(f"Warning: Could not parse S3 URL: {s3_url}")
            return s3_url

        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )

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