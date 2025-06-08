# Import necessary libraries
import os      # For operating system related functions like file path operations and environment variable access
import boto3   # AWS SDK for Python, used to interact with AWS services


# Initialize S3 client
# Used for interacting with Amazon S3 storage service (upload, download, list files, etc.)
s3 = boto3.client("s3")

# Configuration constants and environment variables
# S3 prefix path for model files, all model files are stored under "models/" directory
MODEL_PREFIX = "models/"

# Get model filename from environment variable, use default value if not set
# This allows using different model files in different environments for flexibility
MODEL_ENV = os.environ.get("MODEL_NAME", "BirdNET_Model.tflite")

# Get labels filename from environment variable, use default value if not set
# Labels file contains all bird species names that BirdNET model can recognize
LABELS_FILE = os.environ.get("LABELS_FILE", "BirdNET_Labels.txt")

DDB_TABLE = os.environ.get("DDB_TABLE", "bird-db")  # DynamoDB table name for storing detection results

# Get S3 bucket name from environment variable, use default value if not set
# This is the S3 bucket that stores model files and labels files
BUCKET_NAME = os.environ.get("BUCKET_NAME", "birdtag-models-fit5225-g138")

def download_file_from_s3(bucket, key, download_path):
    """
    Download file from specified S3 bucket to local path
    
    Args:
        bucket (str): S3 bucket name
        key (str): S3 file key (equivalent to file path)
        download_path (str): Local download path
    
    Purpose:
        - This is a generic file download function
        - Can download any S3 file (audio files, model files, etc.)
        - Usually used to download audio files that trigger Lambda
    """
    s3.download_file(bucket, key, download_path)

def get_model_path():
    """
    Get local path of machine learning model file, download from S3 if file doesn't exist
    
    Returns:
        str: Local path of model file (located in /tmp directory)
    
    Workflow:
        1. Build model file path in Lambda /tmp directory
        2. Check if file already exists (caching mechanism)
        3. If doesn't exist, download model file from S3
        4. Return local file path for model loading
    
    Optimization features:
        - Caching mechanism: avoid repeatedly downloading large files
        - Use /tmp directory: the only writable filesystem in Lambda functions
        - Container reuse: multiple calls of same Lambda container instance can share cache
    """
    # Build complete path for model file in Lambda /tmp directory
    # /tmp is the only writable directory in Lambda functions, providing 512MB-10GB temporary storage
    tmp_path = os.path.join("/tmp", MODEL_ENV)

    # Check if model file already exists in local cache
    if not os.path.exists(tmp_path):
        # If file doesn't exist, download from S3
        print(f"Downloading model {MODEL_ENV} from s3://{BUCKET_NAME}/{MODEL_PREFIX}{MODEL_ENV}")
        
        # Download model file from S3 to local /tmp directory
        # Complete S3 path is: BUCKET_NAME/models/MODEL_ENV
        s3.download_file(BUCKET_NAME, MODEL_PREFIX + MODEL_ENV, tmp_path)
    else:
        # If file already exists, use cached file directly
        print("Model already cached in /tmp")

    # Return local path of model file for subsequent model loading
    return tmp_path

def get_labels_file_path():
    """
    Get local path of BirdNET labels file, download from S3 if file doesn't exist
    
    Returns:
        str: Local path of labels file (located in /tmp directory)
    
    Workflow:
        1. Build labels file path in Lambda /tmp directory
        2. Check if file already exists (caching mechanism)
        3. If doesn't exist, download labels file from S3
        4. Return local file path for label reading
    
    Optimization features:
        - Caching mechanism: avoid repeatedly downloading labels file
        - Use /tmp directory: the only writable filesystem in Lambda functions
        - Container reuse: multiple calls of same Lambda container instance can share cache
        - Consistent design pattern with model download
    """
    # Build complete path for labels file in Lambda /tmp directory
    # Use environment variable configured labels filename for deployment flexibility
    tmp_path = os.path.join("/tmp", LABELS_FILE)

    # Check if labels file already exists in local cache
    if not os.path.exists(tmp_path):
        # If file doesn't exist, download from S3
        print(f"Downloading labels file {LABELS_FILE} from s3://{BUCKET_NAME}/{MODEL_PREFIX}{LABELS_FILE}")
        
        # Download labels file from S3 to local /tmp directory
        # Complete S3 path is: BUCKET_NAME/models/LABELS_FILE
        s3.download_file(BUCKET_NAME, MODEL_PREFIX + LABELS_FILE, tmp_path)
    else:
        # If file already exists, use cached file directly
        print("Labels file already cached in /tmp")

    # Return local path of labels file for subsequent label processing
    return tmp_path


def write_to_dynamodb(media_id, species_count, file_type, full_url):
    """
    Write bird species detection results to DynamoDB table using batch writer
    
    Args:
        media_id (str): Unique identifier for the media file
        species_count (dict): Dictionary of detected bird species and their counts
        file_type (str): Type of media file (e.g., 'audio')
        full_url (str): Full S3 URL of the processed file
    
    Purpose:
        - Store detection results in DynamoDB for later retrieval
        - Use batch writer for efficient bulk insertions
        - Create one record per detected bird species
    """
    # Initialize DynamoDB resource
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DDB_TABLE)
    
    try:
        with table.batch_writer() as batch:
            for bird_tag, count in species_count.items():
                batch.put_item(Item={
                    'media_id': media_id,
                    'bird_tag': bird_tag,
                    'count': count,
                    'file_type': file_type,
                    'full_url': full_url
                })
        
        print(f"Successfully wrote {len(species_count)} species to DynamoDB for {media_id}")
        
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")
        raise