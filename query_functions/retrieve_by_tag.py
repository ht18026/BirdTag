import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer

# Initialize DynamoDB client and TypeDeserializer
dynamodb_client = boto3.client('dynamodb')
deserializer = TypeDeserializer()

# Environment variables for table and GSI names (set these in your Lambda configuration)
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db-Shuyang') # Replace 'bird-db-Shuyang' with your actual table name
GSI_NAME = os.environ.get('DYNAMODB_GSI_NAME', 'bird_tag-index')       # Replace 'birdTagIndex' with your actual GSI name

def lambda_handler(event, context):
    try:
        # 1. Parse the input JSON from the request body
        try:
            # Assuming input is a list of bird tags, e.g., ["crow", "pigeon"]
            request_body = json.loads(event.get('body', '[]'))
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in request body'})
            }

        # Ensure the parsed body is a list
        if not isinstance(request_body, list):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body must be a list of bird tags.'})
            }

        if not request_body:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body is empty. Please provide bird tags.'})
            }

        # 2. For each tag in the input, query the GSI
        #    Store media_ids that satisfy the condition (at least one present) for each tag
        #    Also cache media details to avoid re-fetching
        
        sets_of_media_ids_for_each_tag = []
        media_details_cache = {}  # { "media_id_1": {details}, "media_id_2": {details} }

        # The input is expected to be a list of bird species, e.g., ["crow", "pigeon"]
        for bird_species in request_body: 
            if not isinstance(bird_species, str) or not bird_species.strip():
                # Optionally, you can skip invalid entries or return an error
                print(f"Warning: Invalid or empty bird species tag found: '{bird_species}'. Skipping.")
                continue # Or return a 400 error

            current_tag_media_ids = set()
            
            # Paginate through GSI query results
            paginator = dynamodb_client.get_paginator('query')
            try:
                page_iterator = paginator.paginate(
                    TableName=TABLE_NAME,
                    IndexName=GSI_NAME,
                    KeyConditionExpression="bird_tag = :tag_val",
                    # FilterExpression removed as per requirement
                    ExpressionAttributeValues={
                        ":tag_val": {"S": bird_species}
                    },
                    # Project all necessary attributes. Ensure your GSI is configured to project these.
                    # 'count' is no longer strictly needed for projection if only used for the removed filter,
                    # but keeping it in ProjectionExpression doesn't harm if GSI projects it.
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
                    'body': json.dumps({'error': f'Failed to query data for tag: {bird_species}. Details: {str(e)}'})
                }

            # If any tag yields no results (no media file contains this bird), 
            # the final intersection will be empty.
            if not current_tag_media_ids:
                return {
                    'statusCode': 200,
                    'body': json.dumps({"links": []})
                }
            sets_of_media_ids_for_each_tag.append(current_tag_media_ids)

        # 3. Perform intersection of media_ids
        # This ensures that the resulting media files contain ALL specified bird species.
        if not sets_of_media_ids_for_each_tag: 
            # This case should be covered if request_body is not empty but all queries fail to find tags,
            # or if request_body was empty initially (though that's checked earlier).
            return {
                'statusCode': 200,
                'body': json.dumps({"links": []})
            }

        # Start with the first set and intersect with the rest
        intersected_media_ids = sets_of_media_ids_for_each_tag[0].copy()
        for i in range(1, len(sets_of_media_ids_for_each_tag)):
            intersected_media_ids.intersection_update(sets_of_media_ids_for_each_tag[i])

        # 4. Construct the response with URLs
        result_links = set() # Use a set to avoid duplicate URLs
        for media_id in intersected_media_ids:
            details = media_details_cache.get(media_id)
            if not details:
                print(f"Warning: Details for media_id {media_id} not found in cache. Skipping.")
                continue

            file_type = details.get('file_type')
            
            if file_type == 'image':
                thumb_url = details.get('thumb_url')
                if thumb_url:
                    result_links.add(thumb_url)
            else: # For videos or other types, use the full_url
                full_url = details.get('full_url')
                if full_url:
                    result_links.add(full_url)

        return {
            'statusCode': 200,
            'body': json.dumps({"links": sorted(list(result_links))}) # Sorted for consistent output
        }

    except Exception as e:
        print(f"Unhandled error in lambda_handler: {e}") # Log to CloudWatch
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error. Check Lambda logs for details.'})
        }