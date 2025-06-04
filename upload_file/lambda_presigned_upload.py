import json
import boto3
import os
import uuid
from datetime import datetime
import mimetypes

# Initialize S3 client
s3_client = boto3.client('s3')

# S3 bucket name
S3_BUCKET = os.environ.get('S3_BUCKET_NAME', 'fit5225-lyla-a3')

# Supported file type mappings
FILE_TYPE_MAPPING = {
    # Image files
    'image/jpeg': 'images',
    'image/jpg': 'images',
    'image/png': 'images',
    'image/gif': 'images',
    'image/bmp': 'images',
    'image/webp': 'images',
    'image/tiff': 'images',
    
    # Video files
    'video/mp4': 'videos',
    'video/avi': 'videos',
    'video/mov': 'videos',
    'video/wmv': 'videos',
    'video/flv': 'videos',
    'video/webm': 'videos',
    'video/mkv': 'videos',
    'video/m4v': 'videos',
    'video/quicktime': 'videos',
    
    # Audio files
    'audio/mp3': 'audios',
    'audio/mpeg': 'audios',
    'audio/wav': 'audios',
    'audio/flac': 'audios',
    'audio/aac': 'audios',
    'audio/ogg': 'audios',
    'audio/wma': 'audios',
    'audio/m4a': 'audios',
    
    # Document files
    'application/pdf': 'documents',
    'application/msword': 'documents',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'documents',
    'application/vnd.ms-excel': 'documents',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'documents',
    'text/plain': 'documents',
    'text/csv': 'documents',
    
    # Archive files
    'application/zip': 'archives',
    'application/x-rar-compressed': 'archives',
    'application/x-tar': 'archives',
    'application/gzip': 'archives',
}

def lambda_handler(event, context):
    """
    Lambda function to generate S3 presigned URL
    """
    
    # CORS response headers
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
    }
    
    try:
        
        # Extract required parameters
        file_name = event.get('file_name')
        content_type = event.get('content_type')
        file_size = event.get('file_size', 0)
        
        if not file_name:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'Missing required field: file_name'
                })
            }
        
        # Check file size limit (e.g., limit to 100MB)
        MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
        if file_size > MAX_FILE_SIZE:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': f'File size too large. Maximum allowed: {MAX_FILE_SIZE / 1024 / 1024}MB'
                })
            }
        
        # Determine file type and target folder
        folder_name = determine_file_folder(file_name, content_type)
        
        # Generate unique filename
        file_extension = os.path.splitext(file_name)[1]
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        s3_key = f"{folder_name}/{unique_filename}"
        
        # Generate presigned URL (expires in 15 minutes)
        try:
            presigned_url = s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': S3_BUCKET,
                    'Key': s3_key,
                    'ContentType': content_type or 'application/octet-stream',
                    'Metadata': {
                        'original_name': file_name,
                        'upload_time': datetime.now().isoformat()
                    }
                },
                ExpiresIn=900  # 15 minutes
            )
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'error': f'Failed to generate presigned URL: {str(e)}'})
            }
        
        # Generate file URL
        file_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}"
        
        # Build successful response
        response_data = {
            'presigned_url': presigned_url,
            'file_key': s3_key,
            'file_url': file_url,
            'file_type': folder_name,
            'original_name': file_name,
            'expires_in': 900,
            'message': 'Presigned URL generated successfully'
        }
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': f'Internal server error: {str(e)}'
            })
        }

def determine_file_folder(file_name, content_type=None):
    """
    Determine the folder where the file should be stored based on filename and MIME type
    """
    # Prioritize provided MIME type
    if content_type and content_type in FILE_TYPE_MAPPING:
        return FILE_TYPE_MAPPING[content_type]
    
    # Infer MIME type from file extension
    mime_type, _ = mimetypes.guess_type(file_name)
    if mime_type and mime_type in FILE_TYPE_MAPPING:
        return FILE_TYPE_MAPPING[mime_type]
    
    # Determine directly from file extension
    file_extension = os.path.splitext(file_name)[1].lower()
    
    # Image extensions
    if file_extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff', '.tif']:
        return 'images'
    
    # Video extensions
    elif file_extension in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v']:
        return 'videos'
    
    # Audio extensions
    elif file_extension in ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a']:
        return 'audios'
    
    # Document extensions
    elif file_extension in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.txt', '.csv']:
        return 'documents'
    
    # Archive extensions
    elif file_extension in ['.zip', '.rar', '.tar', '.gz', '.7z']:
        return 'archives'
    
    # Default to others
    else:
        return 'others' 