import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

# Initialize DynamoDB client and (de)serializers
dynamodb_client = boto3.client('dynamodb')
deserializer = TypeDeserializer()
serializer = TypeSerializer()

# Environment variables
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db-Shuyang')
THUMB_URL_GSI_NAME = os.environ.get('DYNAMODB_THUMB_GSI_NAME', 'thumb_url-index')
FULL_URL_GSI_NAME = os.environ.get('DYNAMODB_FULL_GSI_NAME', 'full_url-index')

def _parse_input_tags(tags_str_list):
    """Parses the list of tag strings (e.g., "crow,1") into a list of dicts."""
    parsed_tags = []
    errors = []
    for tag_str in tags_str_list:
        parts = tag_str.split(',')
        if len(parts) == 2:
            species = parts[0].strip()
            try:
                delta = int(parts[1].strip())
                if not species:
                    errors.append(f"Invalid tag string '{tag_str}': Species name cannot be empty.")
                    continue
                parsed_tags.append({'species': species, 'delta': delta})
            except ValueError:
                errors.append(f"Invalid count in tag string '{tag_str}'. Count must be an integer.")
        else:
            errors.append(f"Invalid tag string format '{tag_str}'. Expected 'species,count'.")
    return parsed_tags, errors

def _get_base_media_info_for_urls(url_list):
    """
    Queries GSIs to find unique media_ids and their base info (file_type, full_url, thumb_url).
    Returns a dictionary: {media_id: {'file_type': ..., 'full_url': ..., 'thumb_url': ...}}
    """
    media_info_map = {} # {media_id: base_info}
    
    for url_string in url_list:
        # Query by thumb_url
        try:
            response = dynamodb_client.query(
                TableName=TABLE_NAME,
                IndexName=THUMB_URL_GSI_NAME,
                KeyConditionExpression="thumb_url = :url_val",
                ExpressionAttributeValues={":url_val": {"S": url_string}},
                # Ensure GSI projects these attributes
                ProjectionExpression="media_id, file_type, full_url, thumb_url"
            )
            for item_raw in response.get('Items', []):
                item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                media_id = item.get('media_id')
                if media_id:
                    media_info_map[media_id] = {
                        'file_type': item.get('file_type'),
                        'full_url': item.get('full_url'),
                        'thumb_url': item.get('thumb_url') # This will be the queried thumb_url
                    }
        except Exception as e:
            print(f"Error querying thumbUrlIndex for {url_string}: {e}")
            # Continue to next URL or GSI type

        # Query by full_url
        try:
            response = dynamodb_client.query(
                TableName=TABLE_NAME,
                IndexName=FULL_URL_GSI_NAME,
                KeyConditionExpression="full_url = :url_val",
                ExpressionAttributeValues={":url_val": {"S": url_string}},
                ProjectionExpression="media_id, file_type, full_url, thumb_url"
            )
            for item_raw in response.get('Items', []):
                item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                media_id = item.get('media_id')
                if media_id:
                    # Update or add, ensuring we have the most complete info if found by both
                    # (though base info should be consistent for a given media_id)
                    media_info_map[media_id] = {
                        'file_type': item.get('file_type'),
                        'full_url': item.get('full_url'), # This will be the queried full_url
                        'thumb_url': item.get('thumb_url')
                    }
        except Exception as e:
            print(f"Error querying fullUrlIndex for {url_string}: {e}")
            # Continue
            
    return media_info_map

def lambda_handler(event, context):
    # Sample input format:
    # {
    #     "url": ["https://example.com/image1.jpg", "https://example.com/image2.jpg"],
    #     "operation": 1,  # 0 for remove, 1 for add
    #     "tags": ["crow,1", "sparrow,2"]
    # }
    try:
        request_body = json.loads(event.get('body', '{}'))
    except json.JSONDecodeError:
        return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid JSON in request body.'})}

    input_urls = request_body.get('url')
    operation_type = request_body.get('operation')
    tags_to_modify_str = request_body.get('tags')

    # --- Basic Input Validation ---
    if not isinstance(input_urls, list) or not all(isinstance(u, str) for u in input_urls):
        return {'statusCode': 400, 'body': json.dumps({'error': "'url' must be a list of strings."})}
    if operation_type not in [0, 1]:
        return {'statusCode': 400, 'body': json.dumps({'error': "'operation' must be 0 (remove) or 1 (add)."})}
    if not isinstance(tags_to_modify_str, list) or not all(isinstance(t, str) for t in tags_to_modify_str):
        return {'statusCode': 400, 'body': json.dumps({'error': "'tags' must be a list of strings (e.g., ['crow,1'])."}) }

    parsed_tags, tag_parsing_errors = _parse_input_tags(tags_to_modify_str)
    if tag_parsing_errors:
        return {'statusCode': 400, 'body': json.dumps({'errors': ["Input tag parsing errors:"] + tag_parsing_errors})}
    if not parsed_tags and tags_to_modify_str: # If input had tags but all failed parsing
        return {'statusCode': 400, 'body': json.dumps({'error': 'No valid tags to process after parsing.'})}
    if not parsed_tags: # No tags provided to operate on
         return {'statusCode': 200, 'body': json.dumps({'message': 'No tags specified in the request. No operations performed.', 'success_operations': [], 'failed_operations': []})}


    success_ops_details = []
    failed_ops_details = []

    # 1. Find all unique media_ids and their base info from the input URLs
    #    base_media_info_map: { media_id: {file_type, full_url, thumb_url} }
    base_media_info_map = _get_base_media_info_for_urls(input_urls)

    if not base_media_info_map:
        return {'statusCode': 404, 'body': json.dumps({'error': 'No media records found for the provided URLs.', 'success_operations': [], 'failed_operations': []})}

    # 2. For each identified media_id, and for each tag, perform add/remove
    for media_id, base_info in base_media_info_map.items():
        if not base_info.get('file_type') or not base_info.get('full_url'):
            msg = f"Skipping media_id '{media_id}': Essential base info (file_type or full_url) missing from GSI projection."
            print(msg)
            failed_ops_details.append(msg)
            continue

        for tag_detail in parsed_tags:
            species = tag_detail['species']
            count_delta = tag_detail['delta']
            item_key = {'media_id': {'S': media_id}, 'bird_tag': {'S': species}}

            try:
                if operation_type == 1: # ADD operation
                    update_expression_parts = ["SET #c = if_not_exists(#c, :zero) + :delta"]
                    expression_attribute_names = {"#c": "count"}
                    # Serialize values for DynamoDB
                    expression_attribute_values = {
                        ":delta": serializer.serialize(count_delta),
                        ":zero": serializer.serialize(0),
                        ":ft": serializer.serialize(base_info['file_type']),
                        ":fu": serializer.serialize(base_info['full_url'])
                    }
                    update_expression_parts.append("file_type = if_not_exists(file_type, :ft)")
                    update_expression_parts.append("full_url = if_not_exists(full_url, :fu)")

                    if base_info.get('thumb_url'): # Only set thumb_url if it exists in base_info
                        expression_attribute_values[":tu"] = serializer.serialize(base_info['thumb_url'])
                        update_expression_parts.append("thumb_url = if_not_exists(thumb_url, :tu)")
                    
                    final_update_expression = ", ".join(update_expression_parts)
                    
                    dynamodb_client.update_item(
                        TableName=TABLE_NAME,
                        Key=item_key,
                        UpdateExpression=final_update_expression,
                        ExpressionAttributeNames=expression_attribute_names,
                        ExpressionAttributeValues=expression_attribute_values
                    )
                    success_ops_details.append(f"Media ID '{media_id}', Tag '{species}': Count updated by {count_delta} (Add operation).")

                elif operation_type == 0: # REMOVE operation
                    # Get current item to check count
                    response = dynamodb_client.get_item(TableName=TABLE_NAME, Key=item_key)
                    item_to_remove = response.get('Item')

                    if item_to_remove:
                        current_item_deserialized = {k: deserializer.deserialize(v) for k,v in item_to_remove.items()}
                        current_count = current_item_deserialized.get('count', 0)
                        new_count = current_count - count_delta

                        if new_count <= 0:
                            dynamodb_client.delete_item(TableName=TABLE_NAME, Key=item_key)
                            success_ops_details.append(f"Media ID '{media_id}', Tag '{species}': Removed (count became <= 0).")
                        else:
                            dynamodb_client.update_item(
                                TableName=TABLE_NAME,
                                Key=item_key,
                                UpdateExpression="SET #c = :new_val",
                                ExpressionAttributeNames={"#c": "count"},
                                ExpressionAttributeValues={":new_val": serializer.serialize(new_count)}
                            )
                            success_ops_details.append(f"Media ID '{media_id}', Tag '{species}': Count updated to {new_count} (Remove operation).")
                    else:
                        # Tag not found for this media_id, ignore for removal as per requirement
                        success_ops_details.append(f"Media ID '{media_id}', Tag '{species}': Not found for removal, no action taken.")
            
            except Exception as e:
                err_msg = f"Failed operation for Media ID '{media_id}', Tag '{species}': {str(e)}"
                print(err_msg)
                failed_ops_details.append(err_msg)

    return {
        'statusCode': 200, # Or 207 if you want to signify partial success
        'body': json.dumps({
            'message': 'Tag operations processed.',
            'success_operations': success_ops_details,
            'failed_operations': failed_ops_details
        })
    }