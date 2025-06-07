import json
import base64
import tempfile
import os
import boto3
import numpy as np
from pathlib import Path

def lambda_handler(event, context):
    """
    Dedicated Lambda function for processing audio files
    Receives payload from main function, performs audio analysis, returns detected bird tags
    
    Args:
        event (dict): Event payload containing base64 encoded audio file
        context: Lambda runtime context
        
    Returns:
        dict: Analysis results with detected bird species list
    """
    
    try:
        # Extract file data from event payload
        file_content_b64 = event['file_content']
        filename = event['filename']
        content_type = event['content_type']
        
        print(f"Processing audio file: {filename} ({content_type})")
        
        # Decode base64 file content to binary
        file_content = base64.b64decode(file_content_b64)
        print(f"Decoded file size: {len(file_content)} bytes")
        
        # Create temporary file for audio processing
        temp_path = create_temp_audio_file(file_content, filename)
        
        try:
            # Execute audio analysis using machine learning model
            detected_species = analyze_audio_file(temp_path)
            
            print(f"Analysis completed. Detected {len(detected_species)} species: {list(detected_species.keys())}")
            
            return {
                'detected_species': list(detected_species.keys()),  # Return list of detected bird tags
                'file_type': 'audio',
                'filename': filename,
                'message': f'Audio analysis completed. Detected {len(detected_species)} species.'
            }
            
        finally:
            # Clean up temporary file to free disk space
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"Cleaned up temp file: {temp_path}")
                
    except Exception as e:
        print(f"Audio analysis error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Audio analysis failed: {str(e)}")

def create_temp_audio_file(file_content, filename):
    """
    Create temporary audio file in Lambda's /tmp directory
    
    Args:
        file_content (bytes): Binary audio file content
        filename (str): Original filename to extract extension
        
    Returns:
        str: Path to created temporary file
    """
    # Extract file extension, default to .wav if none provided
    extension = os.path.splitext(filename)[1] or '.wav'
    
    # Create temporary file with appropriate extension
    temp_fd, temp_path = tempfile.mkstemp(
        suffix=extension,
        dir='/tmp',                # Lambda's writable directory
        prefix='audio_'            # Prefix for easy identification
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

def analyze_audio_file(audio_path):
    """
    Core audio analysis logic - uses same audio_prediction function as lambda_function.py
    
    Args:
        audio_path (str): Path to temporary audio file
        
    Returns:
        dict: Dictionary of detected species {species_name: 1}
    """
    try:
        # Get model and labels file paths (download from S3 if needed)
        model_path = get_model_path()
        labels_file_path = get_labels_file_path()
        
        # Execute prediction using TensorFlow Lite model
        detected_species = audio_prediction(audio_path, model_path, labels_file_path)
        
        return detected_species
        
    except Exception as e:
        print(f"Audio prediction error: {e}")
        import traceback
        traceback.print_exc()
        raise

def audio_prediction(audio_path, model_path, labels_file_path, 
                    min_confidence=0.25, num_threads=8):
    """
    Audio bird species identification prediction function using TensorFlow Lite
    Complete implementation referenced from lambda_function.py
    
    Args:
        audio_path (str): Path to audio file for analysis
        model_path (str): Path to TensorFlow Lite model file
        labels_file_path (str): Path to species labels text file
        min_confidence (float): Minimum confidence threshold for detections
        num_threads (int): Number of CPU threads for model inference
        
    Returns:
        dict: Dictionary of detected species {species_name: 1}
    """
    
    # TensorFlow Lite import with fallback options for different environments
    try:
        from tflite_runtime.interpreter import Interpreter
        print("Using tflite_runtime")
    except ImportError:
        try:
            import tensorflow.lite as tflite
            Interpreter = tflite.Interpreter
            print("Using tensorflow.lite")
        except (ImportError, AttributeError):
            try:
                import tensorflow as tf
                Interpreter = tf.lite.Interpreter
                print("Using tf.lite.Interpreter")
            except (ImportError, AttributeError):
                raise ImportError(
                    f"Unable to import TensorFlow Lite Interpreter.\n"
                    f"Please try installing: pip install tflite-runtime"
                )
    
    # Audio processing configuration parameters
    SAMPLE_RATE = 48000         # Target sample rate for audio processing
    SIG_LENGTH = 3.0            # Length of each audio segment in seconds
    SIG_OVERLAP = 0             # Overlap between segments (0 = no overlap)
    SIG_MINLEN = 1.0            # Minimum segment length in seconds
    APPLY_SIGMOID = True        # Apply sigmoid activation to model outputs
    SIGMOID_SENSITIVITY = 1.0   # Sensitivity parameter for sigmoid function
    
    def read_labels(labels_path):
        """
        Read and process bird species labels from file
        
        Args:
            labels_path (str): Path to labels text file
            
        Returns:
            list: List of processed species names
        """
        raw_labels = Path(labels_path).read_text(encoding="utf-8").splitlines()
        
        processed_labels = []
        for label in raw_labels:
            # Convert underscore format to readable names
            if '_' in label:
                common_name = label.split('_', 1)[1]  # Remove prefix before underscore
                processed_labels.append(common_name)
            else:
                processed_labels.append(label)
        
        return processed_labels
    
    def open_audio_file(path, sample_rate=48000):
        """
        Load and preprocess audio file using soundfile library
        
        Args:
            path (str): Path to audio file
            sample_rate (int): Target sample rate
            
        Returns:
            tuple: (signal_array, sample_rate)
        """
        try:
            import soundfile as sf
            from scipy.signal import resample
            
            print(f"Loading audio file: {path}")
            
            # Load audio file as float32 array
            sig, rate = sf.read(path, dtype='float32')
            print(f"Original: rate={rate}, shape={sig.shape}")
            
            # Convert stereo to mono by averaging channels
            if len(sig.shape) > 1:
                sig = np.mean(sig, axis=1)
                print("Converted to mono")
            
            # Resample to target sample rate if needed
            if rate != sample_rate:
                num_samples = int(len(sig) * sample_rate / rate)
                sig = resample(sig, num_samples).astype(np.float32)
                rate = sample_rate
                print(f"Resampled to: rate={rate}, shape={sig.shape}")
                
            return sig, rate
            
        except Exception as e:
            print(f"Error loading audio file: {e}")
            raise
    
    def split_signal(sig, rate, seconds=3.0, overlap=0, minlen=1.0):
        """
        Split audio signal into fixed-length segments for processing
        
        Args:
            sig (np.array): Audio signal array
            rate (int): Sample rate
            seconds (float): Length of each segment in seconds
            overlap (float): Overlap between segments in seconds
            minlen (float): Minimum segment length in seconds
            
        Returns:
            list: List of audio chunks as numpy arrays
        """
        chunksize = int(rate * seconds)        # Samples per chunk
        stepsize = int(rate * (seconds - overlap))  # Step size between chunks
        minsize = int(rate * minlen)           # Minimum chunk size
        
        # Return empty list if signal is too short
        if len(sig) < minsize:
            return []
        
        chunks = []
        for i in range(0, len(sig), stepsize):
            chunk = sig[i:i + chunksize]
            
            # Only process chunks that meet minimum length requirement
            if len(chunk) >= minsize:
                # Pad chunk to target size if needed
                if len(chunk) < chunksize:
                    chunk = np.pad(chunk, (0, chunksize - len(chunk)), 'constant')
                chunks.append(chunk)
            
            # Stop if we've reached the end of the signal
            if i + chunksize >= len(sig):
                break
        
        return chunks
    
    def flat_sigmoid(x, sensitivity=-1, bias=1.0):
        """
        Apply sigmoid activation function to model predictions
        
        Args:
            x (np.array): Raw model predictions
            sensitivity (float): Sensitivity parameter
            bias (float): Bias parameter
            
        Returns:
            np.array: Sigmoid-activated predictions
        """
        transformed_bias = (bias - 1.0) * 10.0
        return 1 / (1.0 + np.exp(sensitivity * np.clip(x + transformed_bias, -20, 20)))
    
    # Step 1: Read and process species labels
    labels = read_labels(labels_file_path)
    print(f"Loaded {len(labels)} species labels")
    print(f"Sample labels: {labels[:3] if len(labels) >= 3 else labels}")
    
    # Step 2: Load and initialize TensorFlow Lite model
    interpreter = Interpreter(model_path=model_path, num_threads=num_threads)
    interpreter.allocate_tensors()
    
    # Get input and output tensor details
    input_details = interpreter.get_input_details()
    output_details = interpreter.get_output_details()
    
    input_layer_index = input_details[0]["index"]
    output_layer_index = output_details[0]["index"]
    
    # Step 3: Load and preprocess audio file
    sig, rate = open_audio_file(audio_path, SAMPLE_RATE)
    chunks = split_signal(sig, rate, SIG_LENGTH, SIG_OVERLAP, SIG_MINLEN)
    
    print(f"Audio processing completed: {len(sig)/rate:.2f}s, split into {len(chunks)} chunks")
    
    # Step 4: Run inference on each audio chunk
    detected_species = {}
    
    for i, chunk in enumerate(chunks):
        try:
            # Prepare input data for model (add batch dimension)
            input_data = np.expand_dims(chunk, axis=0).astype("float32")
            
            # Run model inference
            interpreter.set_tensor(input_layer_index, input_data)
            interpreter.invoke()
            prediction = interpreter.get_tensor(output_layer_index)
            
            # Apply sigmoid activation if configured
            if APPLY_SIGMOID:
                prediction = flat_sigmoid(prediction, sensitivity=-1, bias=SIGMOID_SENSITIVITY)
            
            # Process predictions and extract bird species above confidence threshold
            chunk_detections = 0
            for pred in prediction:
                for k, confidence in enumerate(pred):
                    if confidence >= min_confidence and k < len(labels):
                        species = labels[k]
                        detected_species[species] = 1  # Mark species as detected
                        chunk_detections += 1
            
            # Log detection results for this chunk
            if chunk_detections > 0:
                print(f"Chunk {i+1}: detected {chunk_detections} bird species")
                        
        except Exception as e:
            print(f"Warning: processing chunk {i+1} failed: {e}")
            continue  # Continue with next chunk if one fails
    
    return detected_species

def get_model_path():
    """
    Get audio model file path - uses environment variables for configuration
    Downloads model from S3 if not already cached locally
    
    Returns:
        str: Path to local model file
    """
    # Get model configuration from environment variables
    model_filename = os.environ.get('MODEL_FILENAME', 'BirdNET_Model.tflite')
    model_s3_key = os.environ.get('MODEL_S3_KEY', f'models/{model_filename}')
    model_local_path = os.environ.get('MODEL_LOCAL_PATH', f'/tmp/{model_filename}')
    
    # Download model from S3 if not already cached
    if not os.path.exists(model_local_path):
        s3_client = boto3.client('s3')
        bucket = os.environ.get('MODEL_BUCKET', 'birdtag-models-fit5225-g138-shuyang')
        
        try:
            print(f"Downloading model from s3://{bucket}/{model_s3_key}")
            s3_client.download_file(bucket, model_s3_key, model_local_path)
            print(f"Downloaded audio model to {model_local_path}")
        except Exception as e:
            raise Exception(f"Failed to download audio model from s3://{bucket}/{model_s3_key}: {e}")
    else:
        print(f"Model already cached at {model_local_path}")
    
    return model_local_path

def get_labels_file_path():
    """
    Get labels file path - uses environment variables for configuration
    Downloads labels from S3 if not already cached locally
    
    Returns:
        str: Path to local labels file
    """
    # Get labels configuration from environment variables
    labels_filename = os.environ.get('LABELS_FILENAME', 'BirdNET_Labels.txt')
    labels_s3_key = os.environ.get('LABELS_S3_KEY', f'models/{labels_filename}')
    labels_local_path = os.environ.get('LABELS_LOCAL_PATH', f'/tmp/{labels_filename}')
    
    # Download labels from S3 if not already cached
    if not os.path.exists(labels_local_path):
        s3_client = boto3.client('s3')
        bucket = os.environ.get('MODEL_BUCKET', 'birdtag-models-fit5225-g138-shuyang')
        
        try:
            print(f"Downloading labels from s3://{bucket}/{labels_s3_key}")
            s3_client.download_file(bucket, labels_s3_key, labels_local_path)
            print(f"Downloaded labels to {labels_local_path}")
        except Exception as e:
            raise Exception(f"Failed to download labels from s3://{bucket}/{labels_s3_key}: {e}")
    else:
        print(f"Labels file already cached at {labels_local_path}")
    
    return labels_local_path