import json
import os
import boto3
from urllib.parse import urlparse
from boto3.dynamodb.types import TypeDeserializer

# Initialize AWS clients
dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
deserializer = TypeDeserializer()

# Environment variables
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db')
THUMB_URL_GSI_NAME = os.environ.get('DYNAMODB_THUMB_GSI_NAME', 'thumb_url-index')
FULL_URL_GSI_NAME = os.environ.get('DYNAMODB_FULL_GSI_NAME', 'full_url-index')

# CORS headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'POST, OPTIONS'
}

def parse_s3_url(s3_url):
    if not s3_url:
        return None, None
    try:
        parsed_url = urlparse(s3_url)
        if parsed_url.scheme == 's3':
            return parsed_url.netloc, parsed_url.path.lstrip('/')
        elif parsed_url.scheme in ['http', 'https'] and 'amazonaws.com' in parsed_url.netloc:
            if '.s3.' in parsed_url.netloc:
                parts = parsed_url.netloc.split('.')
                if parts[1] == 's3':
                    return parts[0], parsed_url.path.lstrip('/')
            elif parsed_url.netloc.startswith('s3.'):
                path_parts = parsed_url.path.lstrip('/').split('/', 1)
                return path_parts[0], path_parts[1] if len(path_parts) > 1 else ''
    except Exception as e:
        print(f"Error parsing S3 URL '{s3_url}': {e}")
    return None, None

def _get_items_to_delete_from_urls(url_list):
    s3_objects_to_delete = set()
    dynamodb_keys_to_delete = set()
    processed_media_ids = set()
    matched_input_urls = set()

    gsi_queries = [
        {'IndexName': THUMB_URL_GSI_NAME, 'KeyAttribute': 'thumb_url'},
        {'IndexName': FULL_URL_GSI_NAME, 'KeyAttribute': 'full_url'}
    ]

    for url_string in url_list:
        url_found = False
        for gsi in gsi_queries:
            try:
                paginator = dynamodb_client.get_paginator('query')
                for page in paginator.paginate(
                    TableName=TABLE_NAME,
                    IndexName=gsi['IndexName'],
                    KeyConditionExpression=f"{gsi['KeyAttribute']} = :val",
                    ExpressionAttributeValues={":val": {"S": url_string}},
                    ProjectionExpression="media_id, bird_tag, full_url, thumb_url"
                ):
                    items = page.get('Items', [])
                    if items:
                        url_found = True
                    for raw in items:
                        item = {k: deserializer.deserialize(v) for k, v in raw.items()}
                        mid, tag = item.get('media_id'), item.get('bird_tag')
                        if not mid or not tag:
                            continue
                        processed_media_ids.add(mid)
                        dynamodb_keys_to_delete.add((mid, tag))
                        for url_field in ('full_url', 'thumb_url'):
                            url = item.get(url_field)
                            if url:
                                b, k = parse_s3_url(url)
                                if b and k:
                                    s3_objects_to_delete.add((b, k))
            except Exception as e:
                print(f"GSI query error: {e}")
        if url_found:
            matched_input_urls.add(url_string)

    return s3_objects_to_delete, dynamodb_keys_to_delete, processed_media_ids, matched_input_urls

def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'CORS preflight OK'})
        }

    try:
        body = json.loads(event.get('body', '{}'))
        input_urls = body.get('url')
    except:
        return {'statusCode': 400, 'headers': CORS_HEADERS, 'body': json.dumps({'error': 'Invalid JSON.'})}

    if not isinstance(input_urls, list) or not all(isinstance(u, str) for u in input_urls):
        return {'statusCode': 400, 'headers': CORS_HEADERS, 'body': json.dumps({'error': "'url' must be a list of strings."})}
    if not input_urls:
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': json.dumps({'message': 'No URLs provided.'})}

    s3_success, s3_fail = [], []
    db_success, db_fail = [], []

    s3_objs, db_keys, media_ids, matched_urls = _get_items_to_delete_from_urls(input_urls)
    unmatched = list(set(input_urls) - matched_urls)

    if not media_ids:
        return {
            'statusCode': 404,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'No matches found.',
                's3_deletions_successful': [],
                's3_deletions_failed': [],
                'dynamodb_deletions_successful': [],
                'dynamodb_deletions_failed': [],
                'unmatched_input_urls': input_urls
            })
        }

    # Delete S3
    delete_map = {}
    for bucket, key in s3_objs:
        delete_map.setdefault(bucket, []).append({'Key': key})
    for bucket, keys in delete_map.items():
        for i in range(0, len(keys), 1000):
            batch = keys[i:i+1000]
            try:
                resp = s3_client.delete_objects(Bucket=bucket, Delete={'Objects': batch})
                s3_success.extend([f"s3://{bucket}/{obj['Key']}" for obj in resp.get('Deleted', [])])
                s3_fail.extend([f"s3://{bucket}/{err['Key']} (Error: {err.get('Code')})" for err in resp.get('Errors', [])])
            except Exception as e:
                s3_fail.extend([f"s3://{bucket}/{obj['Key']} (Exception: {str(e)})" for obj in batch])

    # Delete DynamoDB
    delete_reqs = [{'DeleteRequest': {'Key': {'media_id': {'S': mid}, 'bird_tag': {'S': tag}}}} for mid, tag in db_keys]
    for i in range(0, len(delete_reqs), 25):
        batch = delete_reqs[i:i+25]
        try:
            resp = dynamodb_client.batch_write_item(RequestItems={TABLE_NAME: batch})
            unprocessed = resp.get('UnprocessedItems', {}).get(TABLE_NAME, [])
            db_success.extend([
                f"media_id: {req['DeleteRequest']['Key']['media_id']['S']}, bird_tag: {req['DeleteRequest']['Key']['bird_tag']['S']}"
                for req in batch if req not in unprocessed
            ])
            db_fail.extend([
                f"media_id: {req['DeleteRequest']['Key']['media_id']['S']}, bird_tag: {req['DeleteRequest']['Key']['bird_tag']['S']} (Unprocessed)"
                for req in unprocessed
            ])
        except Exception as e:
            db_fail.extend([
                f"media_id: {req['DeleteRequest']['Key']['media_id']['S']}, bird_tag: {req['DeleteRequest']['Key']['bird_tag']['S']} (Exception: {str(e)})"
                for req in batch
            ])

    return {
        'statusCode': 200,
        'headers': CORS_HEADERS,
        'body': json.dumps({
            'message': 'Deletion process completed.',
            's3_objects_identified_for_deletion': len(s3_objs),
            's3_deletions_successful': s3_success,
            's3_deletions_failed': s3_fail,
            'dynamodb_items_identified_for_deletion': len(db_keys),
            'dynamodb_deletions_successful': db_success,
            'dynamodb_deletions_failed': db_fail,
            'unmatched_input_urls': unmatched
        })
    }