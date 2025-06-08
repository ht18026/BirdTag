import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from urllib.parse import urlparse

# AWS Clients
dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
deserializer = TypeDeserializer()

# Env vars
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db')
THUMB_URL_GSI_NAME = os.environ.get('DYNAMODB_THUMB_GSI_NAME', 'thumb_url-index')
PRESIGNED_EXPIRATION = int(os.environ.get('PRESIGNED_URL_EXPIRATION', '3600'))

def lambda_handler(event, context):
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, OPTIONS'
    }

    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'message': 'CORS preflight'})
        }

    try:
        body = json.loads(event['body']) if isinstance(event.get('body'), str) else event
        thumbnail_url = body.get('thumbnail_url', '').strip()

        if not thumbnail_url:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Missing or empty "thumbnail_url" in request.'})
            }

        result = dynamodb_client.query(
            TableName=TABLE_NAME,
            IndexName=THUMB_URL_GSI_NAME,
            KeyConditionExpression="thumb_url = :val",
            ExpressionAttributeValues={":val": {"S": thumbnail_url}},
            ProjectionExpression="full_url, thumb_url, file_type",
            Limit=1
        )

        items = [{k: deserializer.deserialize(v) for k, v in i.items()} for i in result.get('Items', [])]
        if not items:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'error': 'No matching record found.'})
            }

        item = items[0]
        full_url = item.get('full_url')
        thumb_url = item.get('thumb_url')
        file_type = item.get('file_type', 'image')

        presigned_full = generate_presigned_url(full_url)
        presigned_thumb = generate_presigned_url(thumb_url)

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'results': [{
                    'file_type': file_type,
                    'full_url': presigned_full,
                    'thumb_url': presigned_thumb
                }],
                'total_matches': 1
            })
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': str(e)})
        }

def generate_presigned_url(s3_url):
    try:
        bucket, key = parse_s3_url(s3_url)
        if not bucket or not key:
            return s3_url
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=PRESIGNED_EXPIRATION
        )
    except Exception:
        return s3_url

def parse_s3_url(url):
    if not url:
        return None, None
    if url.startswith("s3://"):
        parts = url[5:].split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""
    parsed = urlparse(url)
    if ".s3." in parsed.netloc:
        return parsed.netloc.split(".s3.")[0], parsed.path.lstrip("/")
    if parsed.netloc.startswith("s3."):
        path_parts = parsed.path.lstrip("/").split("/", 1)
        return path_parts[0], path_parts[1] if len(path_parts) > 1 else ""
    return None, None