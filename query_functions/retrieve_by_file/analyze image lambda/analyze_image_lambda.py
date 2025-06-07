import base64
import tempfile
import os
import boto3
from collections import Counter

def lambda_handler(event, context):
    """
    Dedicated Lambda function for processing image files
    Receives payload from main function, performs image analysis, returns detected bird tags
    
    Args:
        event (dict): Event payload containing base64 encoded image file
        context: Lambda runtime context
        
    Returns:
        dict: Analysis results with detected bird species list
    """
    
    try:
        # Extract file data from event payload
        file_content_b64 = event['file_content']
        filename = event['filename']
        content_type = event['content_type']
        
        print(f"Processing image file: {filename} ({content_type})")
        
        # Decode base64 file content to binary
        file_content = base64.b64decode(file_content_b64)
        print(f"Decoded file size: {len(file_content)} bytes")
        
        # Create temporary file for image processing
        temp_path = create_temp_image_file(file_content, filename)
        
        try:
            # Execute image analysis using machine learning model
            detected_species = analyze_image_file(temp_path)
            
            print(f"Analysis completed. Detected {len(detected_species)} species: {list(detected_species.keys()) if detected_species else []}")
            
            return {
                'detected_species': list(detected_species.keys()) if detected_species else [],  # Return list of detected bird tags
                'file_type': 'image',
                'filename': filename,
                'message': f'Image analysis completed. Detected {len(detected_species) if detected_species else 0} species.'
            }
            
        finally:
            # Clean up temporary file to free disk space
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"Cleaned up temp file: {temp_path}")
                
    except Exception as e:
        print(f"Image analysis error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Image analysis failed: {str(e)}")

def create_temp_image_file(file_content, filename):
    """
    Create temporary image file in Lambda's /tmp directory
    
    Args:
        file_content (bytes): Binary image file content
        filename (str): Original filename to extract extension
        
    Returns:
        str: Path to created temporary file
    """
    # Extract file extension, default to .jpg if none provided
    extension = os.path.splitext(filename)[1] or '.jpg'
    
    # Create temporary file with appropriate extension
    temp_fd, temp_path = tempfile.mkstemp(
        suffix=extension,
        dir='/tmp',                # Lambda's writable directory
        prefix='image_'            # Prefix for easy identification
    )
    
    try:
        # Write binary content to temporary file
        with os.fdopen(temp_fd, 'wb') as temp_file:
            temp_file.write(file_content)
        print(f"Created temp file: {temp_path} ({len(file_content)} bytes)")
    except:
        # Close file descriptor if writing fails
        os.close(temp_fd)
        raise
    
    return temp_path

def analyze_image_file(image_path):
    """
    Core image analysis logic - uses same image_prediction function as lambda_function.py
    
    Args:
        image_path (str): Path to temporary image file
        
    Returns:
        dict: Dictionary of detected species {species_name: count}
    """
    try:
        # Get model file path (download from S3 if needed)
        model_path = get_model_path()
        
        # Execute prediction using YOLO model
        detected_species = image_prediction(image_path, model_path)
        
        return detected_species
        
    except Exception as e:
        print(f"Image prediction error: {e}")
        import traceback
        traceback.print_exc()
        raise

def image_prediction(image_path, model_path, confidence=0.5):
    """
    Image bird species identification prediction function using YOLO
    Complete implementation referenced from lambda_function.py
    
    Args:
        image_path (str): Path to image file for analysis
        model_path (str): Path to YOLO model file
        confidence (float): Minimum confidence threshold for detections
        
    Returns:
        dict: Dictionary of detected species {species_name: count}
    """
    
    try:
        # Import YOLO and computer vision libraries
        from ultralytics import YOLO
        import supervision as sv
        import cv2 as cv
        
        print("Using YOLO for image analysis")
        
        # Load YOLO model
        model = YOLO(model_path)
        class_dict = model.names  # Get class names from model
        
        print(f"Loaded YOLO model with {len(class_dict)} classes")
        
        # Load image from local path
        img = cv.imread(image_path)
        
        # Check if image was loaded successfully
        if img is None:
            raise Exception("Couldn't load the image! Please check the image path.")
        
        print(f"Loaded image: {img.shape}")
        
        # Run the model on the image
        result = model(img)[0]
        
        # Convert YOLO result to Detections format
        detections = sv.Detections.from_ultralytics(result)
        
        # Filter detections based on confidence threshold and check if any exist
        if detections.class_id is not None:
            # Apply confidence threshold
            detections = detections[(detections.confidence > confidence)]
            
            # Extract species names from class IDs
            species_list = [class_dict[cls_id] for cls_id in detections.class_id]
            
            # Count occurrences of each species
            species_count = dict(Counter(species_list))
            
            print(f"Detected species: {species_count}")
            return species_count
        else:
            print("No detections found above confidence threshold")
            return {}
            
    except ImportError as e:
        print(f"Missing required libraries: {e}")
        print("Please ensure YOLO dependencies are installed")
        raise Exception(f"Image analysis libraries not available: {e}")
    except Exception as e:
        print(f"Error in image prediction: {e}")
        raise

def get_model_path():
    """
    Get image model file path - uses environment variables for configuration
    Downloads model from S3 if not already cached locally
    
    Returns:
        str: Path to local model file
    """
    # Get model configuration from environment variables
    model_filename = os.environ.get('MODEL_FILENAME', 'model.pt')
    model_s3_key = os.environ.get('MODEL_S3_KEY', f'models/{model_filename}')
    model_local_path = os.environ.get('MODEL_LOCAL_PATH', f'/tmp/{model_filename}')
    
    # Download model from S3 if not already cached
    if not os.path.exists(model_local_path):
        s3_client = boto3.client('s3')
        bucket = os.environ.get('MODEL_BUCKET', 'birdtag-models-fit5225-g138-shuyang')
        
        try:
            print(f"Downloading model from s3://{bucket}/{model_s3_key}")
            s3_client.download_file(bucket, model_s3_key, model_local_path)
            print(f"Downloaded image model to {model_local_path}")
        except Exception as e:
            raise Exception(f"Failed to download image model from s3://{bucket}/{model_s3_key}: {e}")
    else:
        print(f"Model already cached at {model_local_path}")
    
    return model_local_path