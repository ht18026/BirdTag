#!/usr/bin/env python
# coding: utf-8
import os
from collections import Counter, defaultdict
from ultralytics import YOLO
import supervision as sv
import cv2 as cv
from utils import download_file_from_s3, get_model_path


def video_prediction(video_path,model, confidence=0.5,
                     ):
    """
    Function to make predictions on video frames using a trained YOLO model and display the video with annotations.

    Parameters:
        video_path (str): Path to the video file.
    """
    # Load video info and extract  frames per second (fps)
    video_info = sv.VideoInfo.from_video_path(video_path=video_path)
    fps = int(video_info.fps)
    model = YOLO(model)  # Load your custom-trained YOLO model
    tracker = sv.ByteTrack(frame_rate=fps)  # Initialize the tracker with the video's frame rate
    class_dict = model.names  # Get the class labels from the model

    # Capture the video from the given path
    cap = cv.VideoCapture(video_path)
    if not cap.isOpened():
        raise Exception("Error: couldn't open the video!")
    max_species_counts = defaultdict(int)

    # Process the video frame by frame
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:  # End of the video
            break

        # Make predictions on the current frame using the YOLO model
        result = model(frame)[0]
        detections = sv.Detections.from_ultralytics(result)  # Convert model output to Detections format
        detections = tracker.update_with_detections(detections=detections)  # Track detected objects

        # Filter detections based on confidence
        if detections.tracker_id is not None:
            detections = detections[
                (detections.confidence > confidence)]  # Keep detections with confidence greater than a threashold


            # Get list of class names for this frame
            species_names = [class_dict[cls_id] for cls_id in detections.class_id]

            # Count occurrences in this frame
            frame_count = Counter(species_names)

            # Update max counts
            for species, count in frame_count.items():
                max_species_counts[species] = max(max_species_counts[species], count)

    cap.release()
    print("Max species count per frame:", dict(max_species_counts))
    return dict(max_species_counts)


def handler(event, context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        return {"error": "Invalid event format", "detail": str(e)}

    print(f"[INFO] New file uploaded to S3: {bucket}/{key}")

    video_path = f"/tmp/{os.path.basename(key)}"

    download_file_from_s3(bucket, key, video_path)
    model_path = get_model_path()
    result = video_prediction(video_path,model_path)

    return {"tag": result}