import base64
import tempfile
import os
import boto3
from collections import Counter, defaultdict

def lambda_handler(event, context):
    """
    Dedicated Lambda function for processing video files
    Receives payload from main function, performs video analysis, returns detected bird tags
    
    Args:
        event (dict): Event payload containing base64 encoded video file
        context: Lambda runtime context
        
    Returns:
        dict: Analysis results with detected bird species list
    """
    
    try:
        # Extract file data from event payload
        file_content_b64 = event['file_content']
        filename = event['filename']
        content_type = event['content_type']
        
        print(f"Processing video file: {filename} ({content_type})")
        
        # Decode base64 file content to binary
        file_content = base64.b64decode(file_content_b64)
        print(f"Decoded file size: {len(file_content)} bytes")
        
        # Create temporary file for video processing
        temp_path = create_temp_video_file(file_content, filename)
        
        try:
            # Execute video analysis using machine learning model
            detected_species = analyze_video_file(temp_path)
            
            print(f"Analysis completed. Detected {len(detected_species)} species: {list(detected_species.keys()) if detected_species else []}")
            
            return {
                'detected_species': list(detected_species.keys()) if detected_species else [],  # Return list of detected bird tags
                'file_type': 'video',
                'filename': filename,
                'message': f'Video analysis completed. Detected {len(detected_species) if detected_species else 0} species.'
            }
            
        finally:
            # Clean up temporary file to free disk space
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"Cleaned up temp file: {temp_path}")
                
    except Exception as e:
        print(f"Video analysis error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Video analysis failed: {str(e)}")

def create_temp_video_file(file_content, filename):
    """
    Create temporary video file in Lambda's /tmp directory
    
    Args:
        file_content (bytes): Binary video file content
        filename (str): Original filename to extract extension
        
    Returns:
        str: Path to created temporary file
    """
    # Extract file extension, default to .mp4 if none provided
    extension = os.path.splitext(filename)[1] or '.mp4'
    
    # Create temporary file with appropriate extension
    temp_fd, temp_path = tempfile.mkstemp(
        suffix=extension,
        dir='/tmp',                # Lambda's writable directory
        prefix='video_'            # Prefix for easy identification
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

def analyze_video_file(video_path):
    """
    Core video analysis logic - uses same video_prediction function as lambda_function.py
    
    Args:
        video_path (str): Path to temporary video file
        
    Returns:
        dict: Dictionary of detected species {species_name: max_count}
    """
    try:
        # Get model file path (download from S3 if needed)
        model_path = get_model_path()
        
        # Execute prediction using YOLO model
        detected_species = video_prediction(video_path, model_path)
        
        return detected_species
        
    except Exception as e:
        print(f"Video prediction error: {e}")
        import traceback
        traceback.print_exc()
        raise

def video_prediction(video_path, model_path, confidence=0.5):
    """
    Video bird species identification prediction function using YOLO
    Complete implementation referenced from lambda_function.py
    
    Args:
        video_path (str): Path to video file for analysis
        model_path (str): Path to YOLO model file
        confidence (float): Minimum confidence threshold for detections
        
    Returns:
        dict: Dictionary of detected species {species_name: max_count}
    """
    
    try:
        # Import YOLO and computer vision libraries
        from ultralytics import YOLO
        import supervision as sv
        import cv2 as cv
        
        print("Using YOLO for video analysis")
        
        # Load YOLO model
        model = YOLO(model_path)
        class_dict = model.names  # Get class names from model
        
        print(f"Loaded YOLO model with {len(class_dict)} classes")
        
        # Load video info and extract frames per second (fps)
        video_info = sv.VideoInfo.from_video_path(video_path=video_path)
        fps = int(video_info.fps)
        print(f"Video info: {video_info.width}x{video_info.height}, {fps} FPS, {video_info.total_frames} frames")
        
        # Initialize tracker with the video's frame rate
        tracker = sv.ByteTrack(frame_rate=fps)
        
        # Capture the video from the given path
        cap = cv.VideoCapture(video_path)
        if not cap.isOpened():
            raise Exception("Error: couldn't open the video!")
        
        # Track maximum species counts across all frames
        max_species_counts = defaultdict(int)
        frame_count = 0
        
        print("Starting frame-by-frame analysis...")
        
        # Process the video frame by frame (NO frame skipping - process every frame)
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:  # End of the video
                break
            
            frame_count += 1
            
            try:
                # Make predictions on the current frame using the YOLO model
                result = model(frame)[0]
                detections = sv.Detections.from_ultralytics(result)  # Convert model output to Detections format
                detections = tracker.update_with_detections(detections=detections)  # Track detected objects
                
                # Filter detections based on confidence threshold
                if detections.tracker_id is not None:
                    detections = detections[(detections.confidence > confidence)]  # Keep detections with confidence greater than threshold
                    
                    # Get list of class names for this frame
                    species_names = [class_dict[cls_id] for cls_id in detections.class_id]
                    
                    # Count occurrences in this frame
                    frame_species_count = Counter(species_names)
                    
                    # Update max counts
                    for species, count in frame_species_count.items():
                        max_species_counts[species] = max(max_species_counts[species], count)
                    
                    # Log progress periodically (but process every frame)
                    if frame_count % 100 == 0:  # Log every 100 frames
                        print(f"Processed frame {frame_count}, current detections: {dict(frame_species_count)}")
                
            except Exception as e:
                print(f"Warning: Error processing frame {frame_count}: {e}")
                continue  # Continue with next frame if one fails
        
        cap.release()
        
        result_dict = dict(max_species_counts)
        print(f"Video analysis completed. Max species count per frame: {result_dict}")
        
        return result_dict
        
    except ImportError as e:
        print(f"Missing required libraries: {e}")
        print("Please ensure YOLO and video processing dependencies are installed")
        raise Exception(f"Video analysis libraries not available: {e}")
    except Exception as e:
        print(f"Error in video prediction: {e}")
        raise

def get_model_path():
    """
    Get video model file path - uses environment variables for configuration
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
            print(f"Downloaded video model to {model_local_path}")
        except Exception as e:
            raise Exception(f"Failed to download video model from s3://{bucket}/{model_s3_key}: {e}")
    else:
        print(f"Model already cached at {model_local_path}")
    
    return model_local_path