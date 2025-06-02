import base64
import cv2
import numpy as np
import os
import boto3
import json
from urllib.parse import unquote_plus

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
    # 获取S3事件信息
    s3_client = boto3.client('s3')
    
    # 从S3事件中获取bucket和key信息
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    try:
        # 从S3下载图片
        response = s3_client.get_object(Bucket=bucket, Key=key)
        image_content = response['Body'].read()
        
        # 将图片内容转换为OpenCV格式
        nparr = np.frombuffer(image_content, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid image data.')
            }
        
        # 调整图片大小
        resized_img = resize_image(img, max_size=128)
        
        # 将调整后的图片编码为JPEG
        _, buffer = cv2.imencode('.jpg', resized_img, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
        
        # 生成缩略图的key（在原文件名前加上'thumbnails/'）
        thumbnail_key = f"thumbnails/{os.path.basename(key)}"
        
        # 将缩略图上传到S3
        s3_client.put_object(
            Bucket=bucket,
            Key=thumbnail_key,
            Body=buffer.tobytes(),
            ContentType='image/jpeg'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thumbnail created successfully',
                'thumbnail_key': thumbnail_key
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing image: {str(e)}')
        }
