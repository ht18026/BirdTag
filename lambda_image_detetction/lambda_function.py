#!/usr/bin/env python
# coding: utf-8
import os
from collections import Counter

from ultralytics import YOLO
import supervision as sv
import cv2 as cv
import boto3

s3 = boto3.client("s3")
MODEL_PREFIX = "models/"
REGION = "us-east-1"
DDB_TABLE = "bird-db"
MODEL_ENV = os.environ.get("MODEL_NAME", "model.pt")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "fit5225-lyla-a3")

def get_model_path():
    tmp_path = os.path.join("/tmp", MODEL_ENV)

    if not os.path.exists(tmp_path):
        print(f"Downloading model {MODEL_ENV} from s3://{BUCKET_NAME}/{MODEL_ENV}")
        s3.download_file(BUCKET_NAME, MODEL_PREFIX + MODEL_ENV, tmp_path)
    else:
        print("Model already cached in /tmp")

    return tmp_path

def write_to_dynamodb(media_id, species_count, file_type, full_url, thumb_url):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DDB_TABLE)
    with table.batch_writer() as batch:
        for bird_tag, count in species_count.items():
            batch.put_item(Item={
                'media_id': media_id,
                'bird_tag': bird_tag,
                'count': count,
                'file_type': file_type,
                'full_url': full_url,
                'thumb_url': thumb_url
            })

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

def resize_image(img, max_size=128):
    height, width = img.shape[:2]
    if width > height:
        scale = max_size / width
    else:
        scale = max_size / height
    if scale >= 1:
        return img
    new_size = (int(width * scale), int(height * scale))
    resized = cv.resize(img, new_size, interpolation=cv.INTER_AREA)
    return resized

def handler(event, context):
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        return {"error": "Invalid event format", "detail": str(e)}

    print(f"[INFO] New file uploaded to S3: {bucket}/{key}")

    image_path = f"/tmp/{os.path.basename(key)}"
    thumb_path = f"/tmp/thumb_{os.path.basename(key)}"
    file_ext = os.path.splitext(key)[1].lower()
    file_type = "image" if file_ext in [".jpg", ".jpeg", ".png"] else "unknown"

    #download image from s3
    s3.download_file(bucket, key, image_path)

    #generate thumbnail
    img = cv.imread(image_path)
    thumb = resize_image(img)
    cv.imwrite(thumb_path, thumb, [int(cv.IMWRITE_JPEG_QUALITY), 80])

    #upload thumbnail to s3
    thumb_key = key.replace("images/", "thumbs/")
    s3.upload_file(thumb_path, bucket, thumb_key, ExtraArgs={'ContentType': 'image/jpeg'})

    #image detection
    model_path = get_model_path()
    species_count = image_prediction(image_path,model_path)

    #construct urls
    full_url = f"https://{bucket}.s3.{REGION}.amazonaws.com/{key}"
    thumb_url = f"https://{bucket}.s3.{REGION}.amazonaws.com/{thumb_key}"

    #Write to DynamoDB
    write_to_dynamodb(
        media_id=key,
        species_count=species_count,
        file_type=file_type,
        full_url=full_url,
        thumb_url=thumb_url
    )

    return {"message": "Processing complete", "tags": species_count}