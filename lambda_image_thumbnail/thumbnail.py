import base64
import cv2
import numpy as np
import os

def resize_image(img, max_size=128):
    height, width = img.shape[:2]
    if width > height:
        scale = max_size / width
    else:
        scale = max_size / height
    if scale >= 1:
        return img
    new_size = (int(width * scale), int(height * scale))
    resized = cv2.resize(img, new_size, interpolation=cv2.INTER_AREA)
    return resized

def lambda_handler(event, context):
    # Get base64 image data from event
    body = event.get('body')
    if not body:
        return {'statusCode': 400, 'body': 'No image data provided.'}
    img_data = base64.b64decode(body)
    nparr = np.frombuffer(img_data, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return {'statusCode': 400, 'body': 'Invalid image data.'}

    resized_img = resize_image(img, max_size=128)
    # Encode as JPEG and return as base64
    _, buffer = cv2.imencode('.jpg', resized_img, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
    jpg_as_text = base64.b64encode(buffer).decode('utf-8')
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': jpg_as_text
    }
