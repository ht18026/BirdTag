#!/usr/bin/env python
# coding: utf-8
import os
from collections import Counter
from ultralytics import YOLO
import supervision as sv
import cv2 as cv
from utils import download_file_from_s3, get_model_path


def image_prediction(image_path,model, confidence=0.5):
    """
    Function to display predictions of a pre-trained YOLO model on a given image.

    Parameters:
        image_path (str): Path to the image file. Can be a local path or a URL.
        confidence (float): 0-1, only results over this value are saved.
        model (str): path to the model.
    """

    # Load YOLO model
    model = YOLO(model)
    class_dict = model.names

    # Load image from local path
    img = cv.imread(image_path)

    # Check if image was loaded successfully
    if img is None:
        print("Couldn't load the image! Please check the image path.")
        return

    # Run the model on the image
    result = model(img)[0]

    # Convert YOLO result to Detections format
    detections = sv.Detections.from_ultralytics(result)

    # Filter detections based on confidence threshold and check if any exist
    if detections.class_id is not None:
        detections = detections[(detections.confidence > confidence)]
        species_list = [class_dict[cls_id] for cls_id in detections.class_id]
        species_count = dict(Counter(species_list))
        print(species_count)
        return species_count

def handler(event, context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        return {"error": "Invalid event format", "detail": str(e)}

    print(f"[INFO] New file uploaded to S3: {bucket}/{key}")
    image_path = f"/tmp/{os.path.basename(key)}"
    download_file_from_s3(bucket, key, image_path)
    model_path = get_model_path()
    result = image_prediction(image_path,model_path)

    return {"tag": result}