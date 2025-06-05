#!/usr/bin/env python
# coding: utf-8
import os
from utils import download_file_from_s3, get_model_path, get_labels_file_path
import numpy as np
import librosa
from pathlib import Path

def audio_prediction(audio_path, model_path, labels_file_path, 
                    min_confidence=0.25, num_threads=8):
    """
    Audio bird species identification prediction function with fixed label formatting
    """
    
    # TensorFlow Lite import code remains unchanged...
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
    
    # Configuration parameters
    SAMPLE_RATE = 48000
    SIG_LENGTH = 3.0
    SIG_OVERLAP = 0
    SIG_MINLEN = 1.0
    APPLY_SIGMOID = True
    SIGMOID_SENSITIVITY = 1.0
    
    def read_labels(labels_path):
        """Read label file and process format"""
        raw_labels = Path(labels_path).read_text(encoding="utf-8").splitlines()
        
        # Process label format: extract "House Crow" from "Corvus splendens_House Crow"
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
        """Open audio file using librosa"""
        sig, rate = librosa.load(
            path, sr=sample_rate, mono=True, res_type="kaiser_fast"
        )
        return sig, rate
    
    def split_signal(sig, rate, seconds=3.0, overlap=0, minlen=1.0):
        """Split signal into 3-second segments"""
        chunksize = int(rate * seconds)
        stepsize = int(rate * (seconds - overlap))
        minsize = int(rate * minlen)
        
        if len(sig) < minsize:
            return []
        
        chunks = []
        for i in range(0, len(sig), stepsize):
            chunk = sig[i:i + chunksize]
            if len(chunk) >= minsize:
                if len(chunk) < chunksize:
                    chunk = np.pad(chunk, (0, chunksize - len(chunk)), 'constant')
                chunks.append(chunk)
            if i + chunksize >= len(sig):
                break
        
        return chunks
    
    def flat_sigmoid(x, sensitivity=-1, bias=1.0):
        """Apply sigmoid activation function"""
        transformed_bias = (bias - 1.0) * 10.0
        return 1 / (1.0 + np.exp(sensitivity * np.clip(x + transformed_bias, -20, 20)))
    
    # 1. Read and process labels
    labels = read_labels(labels_file_path)
    print(f"Loaded {len(labels)} species labels")
    print(f"Sample labels: {labels[:3] if len(labels) >= 3 else labels}")
    
    # 2. Load main model
    interpreter = Interpreter(model_path=model_path, num_threads=num_threads)
    interpreter.allocate_tensors()
    
    input_details = interpreter.get_input_details()
    output_details = interpreter.get_output_details()
    
    input_layer_index = input_details[0]["index"]
    output_layer_index = output_details[0]["index"]
    
    # 3. Read and process audio
    sig, rate = open_audio_file(audio_path, SAMPLE_RATE)
    chunks = split_signal(sig, rate, SIG_LENGTH, SIG_OVERLAP, SIG_MINLEN)
    
    print(f"Audio processing completed: {len(sig)/rate:.2f}s, split into {len(chunks)} chunks")
    
    # 4. Predict each chunk
    detected_species = {}
    
    for i, chunk in enumerate(chunks):
        try:
            # Prepare input data
            input_data = np.expand_dims(chunk, axis=0).astype("float32")
            
            # Execute prediction
            interpreter.set_tensor(input_layer_index, input_data)
            interpreter.invoke()
            prediction = interpreter.get_tensor(output_layer_index)
            
            # Apply sigmoid activation
            if APPLY_SIGMOID:
                prediction = flat_sigmoid(prediction, sensitivity=-1, bias=SIGMOID_SENSITIVITY)
            
            # Extract species above threshold
            chunk_detections = 0
            for pred in prediction:
                for k, confidence in enumerate(pred):
                    if confidence >= min_confidence and k < len(labels):
                        species = labels[k]  # Now already in processed format
                        detected_species[species] = 1
                        chunk_detections += 1
            
            if chunk_detections > 0:
                print(f"Chunk {i+1}: detected {chunk_detections} bird species")
                        
        except Exception as e:
            print(f"Warning: processing chunk {i+1} failed: {e}")
            continue
    
    return detected_species




def handler(event, context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        return {"error": "Invalid event format", "detail": str(e)}

    print(f"[INFO] New file uploaded to S3: {bucket}/{key}")
    audio_path = f"/tmp/{os.path.basename(key)}"
    download_file_from_s3(bucket, key, audio_path)
    model_path = get_model_path()
    labels_file_path = get_labels_file_path()
    result = audio_prediction(audio_path, model_path, labels_file_path)

    return {"tag": result}