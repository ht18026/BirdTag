#!/usr/bin/env python
# coding: utf-8

import os
import urllib.parse

from utils import download_file_from_s3, get_model_path, get_labels_file_path, write_to_dynamodb
import numpy as np
from pathlib import Path

def audio_prediction(audio_path, model_path, labels_file_path, 
                    min_confidence=0.25, num_threads=8):
    """
    Audio bird species identification prediction function using TensorFlow Lite
    
    Args:
        audio_path (str): Path to the audio file to analyze
        model_path (str): Path to the TensorFlow Lite model file
        labels_file_path (str): Path to the species labels file
        min_confidence (float): Minimum confidence threshold for species detection (default: 0.25)
        num_threads (int): Number of threads for TensorFlow Lite inference (default: 8)
    
    Returns:
        dict: Dictionary of detected bird species and their counts
    
    Workflow:
        1. Import TensorFlow Lite interpreter
        2. Load and process species labels
        3. Initialize the ML model
        4. Load and preprocess audio file
        5. Split audio into overlapping chunks
        6. Run inference on each chunk
        7. Apply confidence thresholding
        8. Return detected species
    """
    
    # TensorFlow Lite import with fallback options
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
            labels_path (str): Path to the labels file
            
        Returns:
            list: Processed list of bird species names
            
        Processing:
            - Extract common names from scientific format
            - Convert "Corvus splendens_House Crow" to "House Crow"
            - Handle labels without underscore format
        """
        raw_labels = Path(labels_path).read_text(encoding="utf-8").splitlines()
        
        # Process label format: extract common name from scientific_common format
        processed_labels = []
        for label in raw_labels:
            if '_' in label:
                # Extract the part after underscore as common name
                common_name = label.split('_', 1)[1]
                processed_labels.append(common_name)
            else:
                # If no underscore, use original label directly
                processed_labels.append(label)
        
        return processed_labels
    
    def open_audio_file(path, sample_rate=48000):
        """
        Load and preprocess audio file using soundfile library
        
        Args:
            path (str): Path to the audio file
            sample_rate (int): Target sample rate for processing
            
        Returns:
            tuple: (audio_signal, sample_rate) as numpy array and integer
            
        Processing steps:
            1. Load audio file using soundfile
            2. Convert stereo to mono by averaging channels
            3. Resample to target sample rate if needed
            4. Return as float32 array
        """
        try:
            import soundfile as sf
            from scipy.signal import resample
            
            print(f"Loading audio file: {path}")
            
            # Read audio file as float32 array
            sig, rate = sf.read(path, dtype='float32')
            print(f"Original: rate={rate}, shape={sig.shape}")
            
            # Convert stereo to mono by averaging channels
            if len(sig.shape) > 1:
                sig = np.mean(sig, axis=1)
                print("Converted to mono")
            
            # Resample to target sample rate if different
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
            rate (int): Sample rate of the audio
            seconds (float): Length of each segment in seconds
            overlap (float): Overlap between segments in seconds
            minlen (float): Minimum segment length in seconds
            
        Returns:
            list: List of audio chunks as numpy arrays
            
        Processing:
            - Calculate chunk size and step size based on sample rate
            - Split signal with sliding window approach
            - Pad short segments to maintain consistent size
            - Skip segments shorter than minimum length
        """
        chunksize = int(rate * seconds)        # Samples per chunk
        stepsize = int(rate * (seconds - overlap))  # Step between chunks
        minsize = int(rate * minlen)           # Minimum chunk size
        
        # Return empty list if signal is too short
        if len(sig) < minsize:
            return []
        
        chunks = []
        for i in range(0, len(sig), stepsize):
            chunk = sig[i:i + chunksize]
            
            # Only process chunks meeting minimum length requirement
            if len(chunk) >= minsize:
                # Pad short chunks to maintain consistent size
                if len(chunk) < chunksize:
                    chunk = np.pad(chunk, (0, chunksize - len(chunk)), 'constant')
                chunks.append(chunk)
            
            # Stop when we've processed the entire signal
            if i + chunksize >= len(sig):
                break
        
        return chunks
    
    def flat_sigmoid(x, sensitivity=-1, bias=1.0):
        """
        Apply sigmoid activation function to model predictions
        
        Args:
            x (np.array): Input predictions
            sensitivity (float): Sensitivity parameter (negative values invert)
            bias (float): Bias parameter for threshold adjustment
            
        Returns:
            np.array: Sigmoid-activated predictions
            
        Purpose:
            - Convert raw model outputs to probability-like values
            - Apply configurable sensitivity and bias adjustments
            - Prevent numerical overflow with clipping
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
    
    # Get input and output tensor details for inference
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
            # Prepare input data: add batch dimension and ensure float32 type
            input_data = np.expand_dims(chunk, axis=0).astype("float32")
            
            # Execute model inference
            interpreter.set_tensor(input_layer_index, input_data)
            interpreter.invoke()
            prediction = interpreter.get_tensor(output_layer_index)
            
            # Apply sigmoid activation if configured
            if APPLY_SIGMOID:
                prediction = flat_sigmoid(prediction, sensitivity=-1, bias=SIGMOID_SENSITIVITY)
            
            # Extract species predictions above confidence threshold
            chunk_detections = 0
            for pred in prediction:
                for k, confidence in enumerate(pred):
                    # Check if confidence meets threshold and index is valid
                    if confidence >= min_confidence and k < len(labels):
                        species = labels[k]  # Get species name from processed labels
                        detected_species[species] = 1  # Mark species as detected
                        chunk_detections += 1
            
            # Log chunk results if species were detected
            if chunk_detections > 0:
                print(f"Chunk {i+1}: detected {chunk_detections} bird species")
                        
        except Exception as e:
            print(f"Warning: processing chunk {i+1} failed: {e}")
            continue
    
    return detected_species


def handler(event, context):
    """
    AWS Lambda handler function for processing S3 audio files
    
    Args:
        event (dict): AWS Lambda event containing S3 trigger information
        context (object): AWS Lambda context object
        
    Returns:
        dict: Processing results including detected species
        
    Workflow:
        1. Parse S3 event to extract bucket and file information
        2. Download audio file from S3 to local temporary storage
        3. Download model and labels files if not cached
        4. Run bird species prediction on audio file
        5. Store results in DynamoDB
        6. Return processing summary
    """
    try:
        # Parse S3 event data with comprehensive debugging
        print(f"[DEBUG] Complete event: {event}")
        
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        
        # URL decode to handle special characters in file names
        decoded_key = urllib.parse.unquote_plus(key)
        
        print(f"[DEBUG] Bucket: '{bucket}'")
        print(f"[DEBUG] Original key: '{key}'")
        print(f"[DEBUG] Decoded key: '{decoded_key}'")
        print(f"[DEBUG] Event name: {record.get('eventName', 'Unknown')}")
        
    except (KeyError, IndexError) as e:
        print(f"[ERROR] Event parsing failed: {e}")
        return {"error": "Invalid event format", "detail": str(e)}

    print(f"[INFO] Processing file: s3://{bucket}/{decoded_key}")

    # Create local file path using basename to avoid directory conflicts
    audio_path = f"/tmp/{os.path.basename(decoded_key)}"
    print(f"[DEBUG] Local path: {audio_path}")
    
    # Download audio file from S3 to Lambda's temporary storage
    download_file_from_s3(bucket, decoded_key, audio_path)

    # Get paths to model and labels files (download if not cached)
    model_path = get_model_path()
    labels_file_path = get_labels_file_path()
    
    # Run bird species prediction on the audio file
    species_count = audio_prediction(audio_path, model_path, labels_file_path)

    # Construct full S3 URL for database storage
    REGION = "us-east-1"
    full_url = f"https://{bucket}.s3.{REGION}.amazonaws.com/{decoded_key}"
    
    # Store detection results in DynamoDB
    write_to_dynamodb(media_id=decoded_key, 
                      species_count=species_count, 
                      file_type="audio", 
                      full_url=full_url)

    # Return processing results
    return {"message": "Processing complete", "tags": species_count}