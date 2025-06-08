import json
import base64
import boto3
import os
from botocore.exceptions import ClientError
from urllib.parse import urlparse

# Initialize Lambda client for invoking other Lambda functions
lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    """
    Main Lambda function: Receives file, calls corresponding analysis Lambda, queries database
    
    Args:
        event: API Gateway event containing multipart file data
        context: Lambda runtime context
        
    Returns:
        dict: Response with detected tags and matching file links
    """
    
    # CORS headers for cross-origin requests
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
        print("=== File Analysis and Retrieval Process Started ===")

        # Parse multipart request data from API Gateway
        file_content, filename, content_type = parse_multipart_request(event)
        print(f"Received file: {filename} ({content_type}, {len(file_content)} bytes)")
        
        # Correction: The real limit is Lambda invoke's 6MB, not 6MB original file
        # Calculate Base64 encoded size
        base64_encoded_size = len(base64.b64encode(file_content))
        json_overhead = 1024  # Reserve space for filename, content_type and other fields
        lambda_invoke_limit = 6 * 1024 * 1024  # 6MB
        
        estimated_payload_size = base64_encoded_size + json_overhead
        
        print(f"File analysis:")
        print(f"  Original file size: {len(file_content)/1024/1024:.2f}MB")
        print(f"  Base64 encoded size: {base64_encoded_size/1024/1024:.2f}MB")
        print(f"  Estimated payload size: {estimated_payload_size/1024/1024:.2f}MB")
        print(f"  Lambda invoke limit: {lambda_invoke_limit/1024/1024:.2f}MB")
        
        # Check if it will exceed Lambda invoke limit
        if estimated_payload_size > lambda_invoke_limit:
            max_supported_file_size = (lambda_invoke_limit - json_overhead) * 3 / 4
            return {
                'statusCode': 413,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'File too large for processing',
                    'details': {
                        'file_size_mb': round(len(file_content)/1024/1024, 2),
                        'encoded_size_mb': round(base64_encoded_size/1024/1024, 2),
                        'payload_size_mb': round(estimated_payload_size/1024/1024, 2),
                        'limit_mb': round(lambda_invoke_limit/1024/1024, 2),
                        'max_supported_file_mb': round(max_supported_file_size/1024/1024, 1)
                    },
                    'message': f'Maximum supported file size: {max_supported_file_size/1024/1024:.1f}MB'
                })
            }
        
        # Call appropriate analysis Lambda function based on file type
        print("=== Starting file analysis ===")
        detected_tags = call_analysis_lambda(file_content, filename, content_type)
        print(f"Analysis completed. Detected tags: {detected_tags}")
        
        # Return early if no bird species detected
        if not detected_tags:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'detected_tags': [],
                    'links': [],
                    'total_matches': 0,
                    'message': 'No bird species detected'
                })
            }
        
        # Query database for files containing the detected tags
        print("=== Starting database query ===")
        result = find_files_by_tags(detected_tags)  # Returns {"links": [...]}
        matching_links = result.get("links", [])    # Extract links array

        # **NEW: Generate presigned URLs for security**
        print("=== Generating presigned URLs ===")
        presigned_expiration = int(os.environ.get('PRESIGNED_URL_EXPIRATION', '3600'))  # Default 1 hour
        presigned_links = generate_presigned_urls_batch(matching_links, presigned_expiration)

        print("=== Process completed successfully ===")
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'detected_tags': detected_tags,
                'links': presigned_links,  # Return presigned URLs instead of direct S3 URLs
                'total_matches': len(matching_links),
                'presigned_expiration': presigned_expiration,
                'message': f'Analysis completed. Found {len(matching_links)} similar files with {presigned_expiration}s access.'
            })
        }
        
    except Exception as e:
        print(f"Error in main lambda: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': 'Processing failed',
                'message': str(e)
            })
        }

def call_analysis_lambda(file_content, filename, content_type):
    """
    Call appropriate analysis Lambda function based on file type
    Add payload size validation
    """
    
    # Prepare payload
    payload = {
        'file_content': base64.b64encode(file_content).decode('utf-8'),
        'filename': filename,
        'content_type': content_type
    }
    
    # Critical check: Validate payload size
    payload_json = json.dumps(payload)
    payload_size = len(payload_json.encode('utf-8'))
    lambda_invoke_limit = 6 * 1024 * 1024  # 6MB limit
    
    print(f"File size: {len(file_content)/1024/1024:.2f}MB")
    print(f"Base64 encoded size: {len(payload['file_content'])/1024/1024:.2f}MB") 
    print(f"Total payload size: {payload_size/1024/1024:.2f}MB")
    print(f"Lambda invoke limit: {lambda_invoke_limit/1024/1024:.2f}MB")
    
    # Check if exceeds limit
    if payload_size > lambda_invoke_limit:
        raise Exception(f"Payload too large for Lambda invoke: {payload_size/1024/1024:.2f}MB > {lambda_invoke_limit/1024/1024:.2f}MB")
    
    # Select Lambda function
    if content_type.startswith('audio/'):
        function_name = os.environ.get('AUDIO_ANALYSIS_FUNCTION', 'analyze_audio_lambda')
    elif content_type.startswith('image/'):
        function_name = os.environ.get('IMAGE_ANALYSIS_FUNCTION', 'analyze_image_lambda')
    elif content_type.startswith('video/'):
        function_name = os.environ.get('VIDEO_ANALYSIS_FUNCTION', 'analyze_video_lambda')
    else:
        raise ValueError(f"Unsupported file type: {content_type}")
    
    try:
        print(f"Calling analysis function: {function_name}")
        print(f"Payload size: {payload_size/1024/1024:.2f}MB")
        
        # Lambda invocation
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=payload_json
        )
        print(f"Lambda response: {response}")
        
        # Parse the response from analysis Lambda
        response_payload = json.loads(response['Payload'].read())
        
        if response['StatusCode'] == 200:
            # Check for errors in the response payload
            if 'errorMessage' in response_payload:
                raise Exception(f"Analysis function error: {response_payload['errorMessage']}")
            
            # Return the detected species list
            return response_payload.get('detected_species', [])
        else:
            raise Exception(f"Analysis function failed with status: {response['StatusCode']}")
            
    except Exception as e:
        print(f"Lambda invoke error: {str(e)}")
        print(f"Function name: {function_name}")
        print(f"Payload size: {payload_size/1024/1024:.2f}MB")
        
        # If payload too large error, provide clearer error message
        if "too large" in str(e).lower() or "request entity too large" in str(e).lower():
            max_file_size = (lambda_invoke_limit - 1024) * 3 / 4  # Subtract JSON overhead
            raise Exception(f"File too large for processing. Maximum supported file size: {max_file_size/1024/1024:.1f}MB")
        
        raise

def parse_multipart_request(event):
    """
    Parse multipart request sent by API Gateway
    Handles both base64 encoded and raw data
    
    Args:
        event (dict): API Gateway event object
        
    Returns:
        tuple: (file_content, filename, content_type)
    """
    
    # Step 1: Get raw multipart data
    if event.get('isBase64Encoded', False):
        # API Gateway performed base64 encoding
        raw_body = base64.b64decode(event['body'])
    else:
        # API Gateway didn't encode, but need to convert to bytes
        body_str = event.get('body', '')
        raw_body = body_str.encode('utf-8') if isinstance(body_str, str) else body_str
    
    # Step 2: Extract boundary from headers
    headers = event.get('headers', {})
    content_type = headers.get('content-type') or headers.get('Content-Type', '')
    
    if 'multipart/form-data' not in content_type:
        raise ValueError("Request must be multipart/form-data")
    
    # Extract boundary parameter
    boundary = None
    for part in content_type.split(';'):
        if 'boundary=' in part:
            boundary = part.split('boundary=')[1].strip()
            break
    
    if not boundary:
        raise ValueError("Multipart boundary not found")
    
    # Step 3: Parse multipart data
    return extract_file_from_multipart(raw_body, boundary)

def extract_file_from_multipart(body, boundary):
    """
    Extract file from multipart data
    
    Args:
        body (bytes): Raw multipart body
        boundary (str): Multipart boundary string
        
    Returns:
        tuple: (file_content, filename, content_type)
    """
    boundary_bytes = boundary.encode('utf-8')
    parts = body.split(b'--' + boundary_bytes)
    
    # Process each part of the multipart data
    for part in parts:
        # Look for file upload part
        if b'Content-Disposition: form-data' in part and b'filename=' in part:
            # Separate headers and file content
            if b'\r\n\r\n' in part:
                headers_section, file_content = part.split(b'\r\n\r\n', 1)
            else:
                continue
            
            # Clean file content (remove trailing boundary)
            file_content = file_content.rstrip(b'\r\n--')
            
            # Parse header information
            headers_str = headers_section.decode('utf-8', errors='ignore')
            
            # Extract filename and content type
            filename = extract_filename(headers_str)
            content_type = extract_content_type(headers_str)
            
            return file_content, filename, content_type
    
    raise ValueError("No file found in multipart data")

def extract_filename(headers):
    """
    Extract filename from Content-Disposition header
    
    Args:
        headers (str): Headers section of multipart part
        
    Returns:
        str: Extracted filename or default name
    """
    for line in headers.split('\n'):
        if 'filename=' in line:
            if 'filename="' in line:
                return line.split('filename="')[1].split('"')[0]
            else:
                return line.split('filename=')[1].split(';')[0].strip()
    return 'uploaded_file'

def extract_content_type(headers):
    """
    Extract Content-Type from headers
    
    Args:
        headers (str): Headers section of multipart part
        
    Returns:
        str: Content-Type or default MIME type
    """
    for line in headers.split('\n'):
        if line.strip().startswith('Content-Type:'):
            return line.split('Content-Type:')[1].strip()
    return 'application/octet-stream'


def find_files_by_tags(detected_tags):
    """
    Find all files in database that contain ALL of the detected tags using GSI query
    Uses intersection operation: only returns files containing all detected tags
    
    Args:
        detected_tags (list): List of bird species names
        
    Returns:
        dict: Dictionary with links array containing matching file URLs
    """
    import boto3
    from boto3.dynamodb.types import TypeDeserializer
    
    # Initialize DynamoDB client and TypeDeserializer
    dynamodb_client = boto3.client('dynamodb')
    deserializer = TypeDeserializer()
    
    # Get table and GSI names from environment variables
    table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'bird-db')
    gsi_name = os.environ.get('DYNAMODB_GSI_NAME', 'bird_tag-index')
    
    if not detected_tags:
        return {"links": []}
    
    try:
        # Store sets of media_ids for each tag (for intersection operation)
        sets_of_media_ids_for_each_tag = []
        media_details_cache = {}  # Cache media details to avoid re-fetching
        
        print(f"Searching for files containing ALL of these tags: {detected_tags}")
        
        # For each detected tag, query the GSI and collect media_ids
        for bird_tag in detected_tags:
            if not isinstance(bird_tag, str) or not bird_tag.strip():
                print(f"Warning: Invalid bird tag found: '{bird_tag}'. Skipping.")
                continue
            
            # Normalize tag (remove extra whitespace)
            normalized_tag = bird_tag.strip()
            print(f"Querying GSI for bird tag: '{normalized_tag}'")
            
            current_tag_media_ids = set()
            
            # Use paginator to handle large result sets
            paginator = dynamodb_client.get_paginator('query')
            
            try:
                # Query GSI with the bird tag as partition key
                page_iterator = paginator.paginate(
                    TableName=table_name,
                    IndexName=gsi_name,
                    KeyConditionExpression="bird_tag = :tag_val",
                    ExpressionAttributeValues={
                        ":tag_val": {"S": normalized_tag}
                    },
                    # Project only the attributes we need for efficiency
                    ProjectionExpression="media_id, file_type, full_url, thumb_url"
                )
                
                # Process each page of results
                for page in page_iterator:
                    for item_raw in page.get('Items', []):
                        # Deserialize DynamoDB item to Python dict
                        item = {k: deserializer.deserialize(v) for k, v in item_raw.items()}
                        
                        media_id = item.get('media_id')
                        if media_id:
                            current_tag_media_ids.add(media_id)
                            
                            # Cache details if not already present
                            if media_id not in media_details_cache:
                                media_details_cache[media_id] = {
                                    'file_type': item.get('file_type'),
                                    'full_url': item.get('full_url'),
                                    'thumb_url': item.get('thumb_url')
                                }
                
                print(f"Found {len(current_tag_media_ids)} files for tag '{normalized_tag}'")
                
                # If any tag has no files, intersection will be empty
                if not current_tag_media_ids:
                    print(f"No files found for tag '{normalized_tag}', intersection will be empty")
                    return {"links": []}
                
                sets_of_media_ids_for_each_tag.append(current_tag_media_ids)
                
            except Exception as e:
                print(f"Error querying DynamoDB for tag '{bird_tag}': {e}")
                # Continue with other tags instead of failing completely
                continue
        
        # Perform intersection operation: find files containing all tags
        if not sets_of_media_ids_for_each_tag:
            print("No valid tag queries found")
            return {"links": []}
        
        # Start with first set and intersect with others
        intersected_media_ids = sets_of_media_ids_for_each_tag[0].copy()
        for i in range(1, len(sets_of_media_ids_for_each_tag)):
            intersected_media_ids.intersection_update(sets_of_media_ids_for_each_tag[i])
            print(f"After intersecting with tag {i+1}, remaining files: {len(intersected_media_ids)}")
        
        print(f"Final intersection contains {len(intersected_media_ids)} files")
        
        # Build response URLs
        result_urls = set()  # Use set to avoid duplicate URLs
        
        for media_id in intersected_media_ids:
            details = media_details_cache.get(media_id)
            if not details:
                print(f"Warning: Details for media_id {media_id} not found in cache. Skipping.")
                continue
            
            file_type = details.get('file_type', '')
            
            # Select appropriate URL based on file type
            if file_type in ['images', 'image']:  # Handle both singular and plural
                url = details.get('thumb_url')  # Use thumbnail for images
                url_type = 'thumbnail'
            else:  # For videos, audio, or other types
                url = details.get('full_url')   # Use full URL for videos/audio
                url_type = 'full'
            
            # Add URL if it exists and is valid
            if url and url.strip() and url.startswith('http'):
                result_urls.add(url)
                print(f"Added {url_type} URL for {file_type} file (media_id: {media_id}): {url}")
        
        # Convert set to sorted list for consistent output
        result_links = sorted(list(result_urls))
        
        print(f"Total unique URLs found that contain ALL tags: {len(result_links)}")
        
        return {"links": result_links}
        
    except Exception as e:
        print(f"Error in find_files_by_tags: {e}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Database query failed: {str(e)}")
    

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

def generate_presigned_urls_batch(urls, expiration=3600):
    """Generate presigned URLs for a list of S3 URLs"""
    if not urls:
        return []
    
    presigned_urls = []
    print(f"Generating presigned URLs for {len(urls)} files")
    
    for url in urls:
        presigned_url = generate_presigned_url(url, expiration)
        presigned_urls.append(presigned_url)
    
    return presigned_urls