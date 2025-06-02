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
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db-Shuyang') # Your DynamoDB table name
THUMB_URL_GSI_NAME = os.environ.get('DYNAMODB_THUMB_GSI_NAME', 'thumbUrlIndex')
FULL_URL_GSI_NAME = os.environ.get('DYNAMODB_FULL_GSI_NAME', 'fullUrlIndex')

def parse_s3_url(s3_url):
    """Parses an S3 URL into bucket and key."""
    try:
        parsed_url = urlparse(s3_url)
        if parsed_url.scheme == 's3':
            bucket_name = parsed_url.netloc
            key = parsed_url.path.lstrip('/')
            return bucket_name, key
        elif parsed_url.scheme in ['http', 'https'] and '.s3.' in parsed_url.netloc:
            # Handles virtual-hosted style URLs like https://bucket.s3.region.amazonaws.com/key
            # or path-style if S3 is part of the path, e.g. https://s3.region.amazonaws.com/bucket/key
            
            # For virtual hosted: bucket.s3.amazonaws.com
            # For path style: s3.amazonaws.com (bucket is first part of path)
            
            host_parts = parsed_url.netloc.split('.')
            if "s3" == host_parts[0] or (len(host_parts) > 1 and "s3" == host_parts[1]): # path-style or s3-region.amazonaws.com
                path_parts = parsed_url.path.lstrip('/').split('/', 1)
                if len(path_parts) >= 1: # Check if path_parts is not empty
                    bucket_name = path_parts[0]
                    key = path_parts[1] if len(path_parts) > 1 else ''
                    return bucket_name, key
            elif "s3" in parsed_url.netloc: # virtual-hosted
                bucket_name = host_parts[0]
                key = parsed_url.path.lstrip('/')
                return bucket_name, key
        return None, None
    except Exception as e:
        print(f"Error parsing S3 URL '{s3_url}': {e}")
        return None, None

def _get_items_to_delete_from_urls(url_list):
    """
    Identifies all DynamoDB items and S3 objects associated with the given URLs.
    Returns:
        s3_objects_to_delete (set): Tuples of (bucket, key) for S3.
        dynamodb_keys_to_delete (set): Tuples of (media_id, bird_tag) for DynamoDB primary keys.
        processed_media_ids (set): media_ids that were found and are valid for processing.
        matched_input_urls (set): Input URLs that returned at least one item from a GSI query.
    """
    s3_objects_to_delete = set()
    dynamodb_keys_to_delete = set() # Stores (media_id, bird_tag) tuples
    processed_media_ids = set() # To track which media_ids we've processed items for
    matched_input_urls = set() # To track input URLs that found any item

    gsi_queries = [
        {'IndexName': THUMB_URL_GSI_NAME, 'KeyAttribute': 'thumb_url'},
        {'IndexName': FULL_URL_GSI_NAME, 'KeyAttribute': 'full_url'}
    ]

    for url_string in url_list:
        url_found_any_item_in_gsi = False # Flag for the current URL across all its GSI queries
        for gsi_config in gsi_queries:
            try:
                paginator = dynamodb_client.get_paginator('query')
                page_iterator = paginator.paginate(
                    TableName=TABLE_NAME,
                    IndexName=gsi_config['IndexName'],
                    KeyConditionExpression=f"{gsi_config['KeyAttribute']} = :url_val",
                    ExpressionAttributeValues={":url_val": {"S": url_string}},
                    # CRUCIAL: GSI must project these attributes
                    ProjectionExpression="media_id, bird_tag, full_url, thumb_url"
                )
                for page in page_iterator:
                    items_in_page = page.get('Items', [])
                    if items_in_page: # If this page has items for the current url_string
                        url_found_any_item_in_gsi = True # Mark that this URL found something in this GSI
                    
                    for item_raw in items_in_page:
                        item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                        
                        media_id = item.get('media_id')
                        bird_tag = item.get('bird_tag')
                        
                        if not media_id or not bird_tag:
                            print(f"Warning: Missing media_id or bird_tag for item found via URL '{url_string}'. Item: {item}")
                            continue
                        
                        processed_media_ids.add(media_id)
                        dynamodb_keys_to_delete.add((media_id, bird_tag))

                        item_full_url = item.get('full_url')
                        item_thumb_url = item.get('thumb_url')

                        if item_full_url:
                            bucket, key = parse_s3_url(item_full_url)
                            if bucket and key:
                                s3_objects_to_delete.add((bucket, key))
                        if item_thumb_url:
                            bucket, key = parse_s3_url(item_thumb_url)
                            if bucket and key: # Key can be empty for bucket root, but unlikely for files
                                s3_objects_to_delete.add((bucket, key))
            except Exception as e:
                print(f"Error querying GSI {gsi_config['IndexName']} for URL '{url_string}': {e}")
                # Continue to next GSI or URL
        
        if url_found_any_item_in_gsi:
            matched_input_urls.add(url_string) # Add if any GSI query for this URL yielded items
                
    return s3_objects_to_delete, dynamodb_keys_to_delete, processed_media_ids, matched_input_urls


def lambda_handler(event, context):
    # Sample input format:
    # {
    #     "url": [
    #         "https://example.com/path/to/image1.jpg", 
    #         "https://example.com/path/to/image2.jpg"
    #     ]

    try:
        request_body = json.loads(event.get('body', '{}'))
        input_urls = request_body.get('url')
    except json.JSONDecodeError:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid JSON in request body.'})}

    if not isinstance(input_urls, list) or not all(isinstance(u, str) for u in input_urls):
        return {'statusCode': 400, 'body': json.dumps({'error': "'url' must be a list of strings."})}
    if not input_urls:
        return {'statusCode': 200, 'body': json.dumps({'message': 'No URLs provided. Nothing to delete.'})}

    s3_delete_success = []
    s3_delete_failure = []
    db_delete_success = []
    db_delete_failure = []
    
    s3_objects_to_delete, dynamodb_keys_to_delete, processed_media_ids, matched_input_urls_set = _get_items_to_delete_from_urls(input_urls)
    
    unmatched_urls = list(set(input_urls) - matched_input_urls_set)

    if not processed_media_ids: # If no valid media_ids were found for ANY URL to process for deletion
        return {
            'statusCode': 404, # Or 200 if this is not considered an error state
            'body': json.dumps({
                'message': 'No matching records found in DynamoDB for any of the provided URLs that could be processed for deletion.',
                's3_deletions_attempted': 0,
                's3_deletions_successful': [],
                's3_deletions_failed': [],
                'dynamodb_deletions_attempted': 0,
                'dynamodb_deletions_successful': [],
                'dynamodb_deletions_failed': [],
                'unmatched_input_urls': input_urls # All input URLs are effectively unmatched if nothing was processed
            })
        }


    # --- Delete S3 Objects ---
    if s3_objects_to_delete:
        # Group by bucket for S3 DeleteObjects API
        s3_batch_delete_map = {}
        for bucket, key in s3_objects_to_delete:
            if bucket not in s3_batch_delete_map:
                s3_batch_delete_map[bucket] = []
            s3_batch_delete_map[bucket].append({'Key': key})

        for bucket_name, objects_in_bucket in s3_batch_delete_map.items():
            # S3 DeleteObjects can take up to 1000 keys
            for i in range(0, len(objects_in_bucket), 1000):
                batch = objects_in_bucket[i:i+1000]
                try:
                    response = s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': batch, 'Quiet': False} # Quiet:False returns info on deleted/errored
                    )
                    if response.get('Deleted'):
                        s3_delete_success.extend([f"s3://{bucket_name}/{d['Key']}" for d in response.get('Deleted', [])])
                    if response.get('Errors'):
                        s3_delete_failure.extend([f"s3://{bucket_name}/{e['Key']} (Error: {e.get('Code')}, {e.get('Message')})" for e in response.get('Errors', [])])
                except Exception as e:
                    s3_delete_failure.extend([f"s3://{bucket_name}/{obj['Key']} (Batch Exception: {str(e)})" for obj in batch])
    
    # --- Delete DynamoDB Items ---
    if dynamodb_keys_to_delete:
        # BatchWriteItem can take up to 25 DeleteRequest items
        delete_requests = [{'DeleteRequest': {'Key': {'media_id': {'S': mid}, 'bird_tag': {'S': btag}}}}
                           for mid, btag in dynamodb_keys_to_delete]
        
        for i in range(0, len(delete_requests), 25):
            batch = delete_requests[i:i+25]
            try:
                response = dynamodb_client.batch_write_item(RequestItems={TABLE_NAME: batch})
                # Handle UnprocessedItems (retry logic can be added here if needed)
                unprocessed = response.get('UnprocessedItems', {}).get(TABLE_NAME, [])
                
                processed_in_batch = len(batch) - len(unprocessed)
                # Assuming success if not in UnprocessedItems for simplicity here
                # A more robust way is to check which specific items were processed.
                # For now, count successes based on what wasn't returned as unprocessed.
                
                # Extract keys from the successfully processed part of the batch
                successful_keys_in_batch = batch[:processed_in_batch]
                db_delete_success.extend([f"media_id: {req['DeleteRequest']['Key']['media_id']['S']}, bird_tag: {req['DeleteRequest']['Key']['bird_tag']['S']}" 
                                          for req in successful_keys_in_batch])

                if unprocessed:
                    db_delete_failure.extend([f"media_id: {item['DeleteRequest']['Key']['media_id']['S']}, bird_tag: {item['DeleteRequest']['Key']['bird_tag']['S']} (Unprocessed)" 
                                              for item in unprocessed])
            except Exception as e:
                db_delete_failure.extend([f"media_id: {req['DeleteRequest']['Key']['media_id']['S']}, bird_tag: {req['DeleteRequest']['Key']['bird_tag']['S']} (Batch Exception: {str(e)})" 
                                          for req in batch])

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Deletion process completed.',
            's3_objects_identified_for_deletion': len(s3_objects_to_delete),
            's3_deletions_successful': s3_delete_success,
            's3_deletions_failed': s3_delete_failure,
            'dynamodb_items_identified_for_deletion': len(dynamodb_keys_to_delete),
            'dynamodb_deletions_successful': db_delete_success,
            'dynamodb_deletions_failed': db_delete_failure,
            'unmatched_input_urls': unmatched_urls
        })
    }