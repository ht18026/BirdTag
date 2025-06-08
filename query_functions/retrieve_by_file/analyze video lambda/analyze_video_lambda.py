import base64
import tempfile
import os
import boto3
import time
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
    Core video analysis logic - uses optimized video_prediction function
    
    Args:
        video_path (str): Path to temporary video file
        
    Returns:
        dict: Dictionary of detected species {species_name: max_count}
    """
    try:
        # Get model file path (download from S3 if needed)
        model_path = get_model_path()
        
        # Execute prediction using YOLO model with smart frame skipping
        detected_species = video_prediction(video_path, model_path)
        
        return detected_species
        
    except Exception as e:
        print(f"Video prediction error: {e}")
        import traceback
        traceback.print_exc()
        raise

def video_prediction(video_path, model_path, confidence=0.5):
    """
    Optimized video bird species identification using YOLO with smart frame skipping
    
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
        
        print("Using YOLO for optimized video analysis with smart frame skipping")
        
        # Load YOLO model
        model = YOLO(model_path)
        class_dict = model.names
        
        print(f"Loaded YOLO model with {len(class_dict)} classes")
        
        # Load video info
        video_info = sv.VideoInfo.from_video_path(video_path=video_path)
        fps = int(video_info.fps)
        total_frames = video_info.total_frames
        duration = total_frames / fps if fps > 0 else 0
        
        print(f"Video info: {video_info.width}x{video_info.height}, {fps} FPS, {total_frames} frames, {duration:.2f}s")
        
        # Smart frame skipping strategy
        frame_skip_interval = calculate_frame_skip_interval(fps, total_frames, duration)
        print(f"Frame skip strategy: analyzing every {frame_skip_interval} frames")
        
        # Initialize tracker with adjusted frame rate
        adjusted_fps = max(1, fps // frame_skip_interval)
        tracker = sv.ByteTrack(frame_rate=adjusted_fps)
        
        # Capture the video
        cap = cv.VideoCapture(video_path)
        if not cap.isOpened():
            raise Exception("Error: couldn't open the video!")
        
        # Track maximum species counts
        max_species_counts = defaultdict(int)
        frame_count = 0
        processed_count = 0
        
        print("Starting optimized frame analysis...")
        start_time = time.time()
        
        # Optimized frame processing loop
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            
            frame_count += 1
            
            # Frame skipping logic - only process frames at specified intervals
            if frame_count % frame_skip_interval != 0:
                continue
            
            processed_count += 1
            
            try:
                # Frame resolution optimization
                optimized_frame = optimize_frame_for_analysis(frame)
                
                # Analyze current frame
                result = model(optimized_frame)[0]
                detections = sv.Detections.from_ultralytics(result)
                detections = tracker.update_with_detections(detections=detections)
                
                # Filter low confidence detections
                if detections.tracker_id is not None:
                    # Dynamic confidence adjustment
                    dynamic_confidence = adaptive_confidence_threshold(processed_count, confidence)
                    detections = detections[(detections.confidence > dynamic_confidence)]
                    
                    # Count species
                    species_names = [class_dict[cls_id] for cls_id in detections.class_id]
                    frame_species_count = Counter(species_names)
                    
                    # Update maximum counts
                    for species, count in frame_species_count.items():
                        max_species_counts[species] = max(max_species_counts[species], count)
                    
                    # Periodic progress reporting
                    if processed_count % 20 == 0:
                        elapsed_time = time.time() - start_time
                        progress = (frame_count / total_frames) * 100
                        print(f"Progress: {progress:.1f}% (frame {frame_count}/{total_frames}, processed {processed_count}, time: {elapsed_time:.1f}s)")
                        if frame_species_count:
                            print(f"Current detections: {dict(frame_species_count)}")
                
                # Early exit strategy - if approaching timeout limit
                elapsed_time = time.time() - start_time
                if elapsed_time > get_max_processing_time():
                    print(f"Approaching timeout limit, stopping analysis at frame {frame_count}")
                    break
                
            except Exception as e:
                print(f"Warning: Error processing frame {frame_count}: {e}")
                continue
        
        cap.release()
        
        elapsed_time = time.time() - start_time
        analysis_ratio = processed_count / total_frames if total_frames > 0 else 0
        
        result_dict = dict(max_species_counts)
        print(f"Video analysis completed in {elapsed_time:.2f}s")
        print(f"Analyzed {processed_count}/{total_frames} frames ({analysis_ratio:.1%})")
        print(f"Max species count: {result_dict}")
        
        # Performance monitoring
        log_performance_metrics(start_time, total_frames, processed_count, result_dict)
        
        return result_dict
        
    except ImportError as e:
        print(f"Missing required libraries: {e}")
        raise Exception(f"Video analysis libraries not available: {e}")
    except Exception as e:
        print(f"Error in video prediction: {e}")
        raise

def calculate_frame_skip_interval(fps, total_frames, duration):
    """
    Calculate optimal frame skip interval to balance analysis quality and processing time
    
    Args:
        fps (int): Video frame rate
        total_frames (int): Total frame count
        duration (float): Video duration in seconds
    
    Returns:
        int: Frame skip interval
    """
    # Get configuration from environment variables
    target_processing_time = int(os.environ.get('TARGET_PROCESSING_TIME', '25'))  # Target processing time
    max_target_frames = int(os.environ.get('MAX_ANALYZED_FRAMES', '300'))  # Maximum target analysis frames
    
    # Adaptive strategy based on video duration
    if duration <= 5:  # Very short video (≤5 seconds)
        skip_interval = max(1, fps // 6)  # Analyze 6 frames per second
        print(f"Short video strategy: {skip_interval} frame skip")
    elif duration <= 15:  # Short video (5-15 seconds)
        skip_interval = max(1, fps // 4)  # Analyze 4 frames per second
        print(f"Medium-short video strategy: {skip_interval} frame skip")
    elif duration <= 30:  # Medium video (15-30 seconds)
        skip_interval = max(1, fps // 2)  # Analyze 2 frames per second
        print(f"Medium video strategy: {skip_interval} frame skip")
    elif duration <= 60:  # Long video (30-60 seconds)
        skip_interval = fps  # Analyze 1 frame per second
        print(f"Long video strategy: {skip_interval} frame skip")
    elif duration <= 120:  # Very long video (60-120 seconds)
        skip_interval = fps * 2  # Analyze 1 frame every 2 seconds
        print(f"Very long video strategy: {skip_interval} frame skip")
    else:  # Extra long video (>120 seconds)
        skip_interval = fps * 3  # Analyze 1 frame every 3 seconds
        print(f"Extra long video strategy: {skip_interval} frame skip")
    
    # Ensure not exceeding target frame count
    estimated_frames = total_frames // skip_interval
    if estimated_frames > max_target_frames:
        skip_interval = max(1, total_frames // max_target_frames)
        print(f"Adjusted skip interval to {skip_interval} to limit frames to {max_target_frames}")
    
    # Ensure reasonable range
    skip_interval = max(1, min(skip_interval, fps * 5))  # Maximum 5 second interval
    
    estimated_final_frames = total_frames // skip_interval
    estimated_time = estimated_final_frames * 0.1  # Estimate 0.1 seconds per frame processing time
    
    print(f"Video analysis plan:")
    print(f"  Duration: {duration:.1f}s, FPS: {fps}, Total frames: {total_frames}")
    print(f"  Skip interval: {skip_interval}")
    print(f"  Frames to analyze: ~{estimated_final_frames}")
    print(f"  Estimated processing time: ~{estimated_time:.1f}s")
    
    return skip_interval

def optimize_frame_for_analysis(frame, target_size=None):
    """
    Optimize frame resolution to improve processing speed
    
    Args:
        frame: Original frame
        target_size: Target size, obtained from environment variables
    
    Returns:
        optimized_frame: Optimized frame
    """
    import cv2 as cv
    
    if target_size is None:
        target_size = int(os.environ.get('TARGET_FRAME_SIZE', '640'))
    
    height, width = frame.shape[:2]
    
    # If original resolution is too high, perform scaling
    if max(width, height) > target_size:
        # Maintain aspect ratio
        if width > height:
            new_width = target_size
            new_height = int(height * target_size / width)
        else:
            new_height = target_size
            new_width = int(width * target_size / height)
        
        optimized_frame = cv.resize(frame, (new_width, new_height), interpolation=cv.INTER_LINEAR)
        return optimized_frame
    
    return frame

def adaptive_confidence_threshold(processed_count, base_confidence=0.5):
    """
    Dynamically adjust confidence threshold based on processing progress
    
    Args:
        processed_count (int): Number of processed frames
        base_confidence (float): Base confidence threshold
    
    Returns:
        float: Adjusted confidence threshold
    """
    # Use lower confidence in early stages to capture more detections
    if processed_count < 30:
        return base_confidence * 0.8
    # Use standard confidence in middle stages
    elif processed_count < 150:
        return base_confidence
    # Increase confidence in later stages to reduce false positives
    else:
        return base_confidence * 1.1

def get_max_processing_time():
    """
    Get maximum processing time, leaving buffer for API Gateway timeout
    
    Returns:
        int: Maximum processing time in seconds
    """
    return int(os.environ.get('MAX_PROCESSING_TIME', '25'))  # Default 25 seconds, leaving 5 seconds buffer for 30-second API Gateway

def log_performance_metrics(start_time, total_frames, processed_frames, detections_found):
    """Log performance metrics"""
    elapsed_time = time.time() - start_time
    fps_processed = processed_frames / elapsed_time if elapsed_time > 0 else 0
    analysis_ratio = processed_frames / total_frames if total_frames > 0 else 0
    
    print(f"=== Performance Metrics ===")
    print(f"Total processing time: {elapsed_time:.2f}s")
    print(f"Frames analyzed: {processed_frames}/{total_frames} ({analysis_ratio:.1%})")
    print(f"Processing speed: {fps_processed:.1f} frames/second")
    print(f"Species detected: {len(detections_found)}")
    if processed_frames > 0:
        print(f"Detection efficiency: {len(detections_found)/processed_frames:.3f} species/frame")

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
            print(f"Downloaded video model to {model_local_path} (size: {os.path.getsize(model_local_path)} bytes)")
        except Exception as e:
            raise Exception(f"Failed to download video model from s3://{bucket}/{model_s3_key}: {e}")
    else:
        print(f"Model already cached at {model_local_path} (size: {os.path.getsize(model_local_path)} bytes)")
    
    return model_local_path