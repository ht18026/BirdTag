#!/usr/bin/env python
# coding: utf-8
from collections import Counter

# requirements
# !pip install ultralytics supervision

from ultralytics import YOLO
import supervision as sv
import cv2 as cv


def image_prediction(image_path, confidence=0.5,
                     model="./model.pt"):
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

if __name__ == '__main__':
    print("predicting...")
    image_prediction("./test_images/crows_1.jpg", result_filename="crows_result1.jpg")
    image_prediction("./test_images/crows_3.jpg", result_filename='crows_detected_2.jpg')
    image_prediction("./test_images/kingfisher_2.jpg",result_filename='kingfishers_detected.jpg' )
    image_prediction("./test_images/myna_1.jpg",result_filename='myna_detected.jpg')
    image_prediction("./test_images/owl_2.jpg",result_filename='owls_detected.jpg')
    image_prediction("./test_images/peacocks_3.jpg",result_filename='peacocks_detected_1.jpg')
    image_prediction('./test_images/sparrow_3.jpg',result_filename='sparrow_detected_1.jpg')
    image_prediction('./test_images/sparrow_1.jpg',result_filename='sparrow_detected_2.jpg')
