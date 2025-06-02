import json
import boto3
from thumbnail import lambda_handler
import os

def create_s3_event(bucket_name, key):
    """创建模拟的S3事件"""
    return {
        'Records': [{
            's3': {
                'bucket': {
                    'name': bucket_name
                },
                'object': {
                    'key': key
                }
            }
        }]
    }

def test_lambda_function():
    # 设置S3客户端
    s3_client = boto3.client('s3')
    
    # 设置测试参数
    bucket_name = 'your-bucket-name'  # 替换为您的S3桶名称
    test_image_path = 'crows_4.jpg'   # 本地测试图片路径
    s3_key = 'uploads/crows_4.jpg'    # S3中的目标路径
    
    try:
        # 上传测试图片到S3
        with open(test_image_path, 'rb') as image_file:
            s3_client.upload_fileobj(
                image_file,
                bucket_name,
                s3_key,
                ExtraArgs={'ContentType': 'image/jpeg'}
            )
        print(f"测试图片已上传到S3: {s3_key}")
        
        # 创建模拟的S3事件
        test_event = create_s3_event(bucket_name, s3_key)
        
        # 调用Lambda函数
        response = lambda_handler(test_event, None)
        
        # 检查响应
        if response['statusCode'] == 200:
            print("测试成功！")
            print(f"响应内容: {response['body']}")
            
            # 验证缩略图是否已创建
            thumbnail_key = json.loads(response['body'])['thumbnail_key']
            try:
                s3_client.head_object(Bucket=bucket_name, Key=thumbnail_key)
                print(f"缩略图已成功创建: {thumbnail_key}")
            except:
                print("错误：缩略图未找到")
        else:
            print(f"测试失败：{response['body']}")
            
    except Exception as e:
        print(f"测试过程中发生错误: {str(e)}")
    finally:
        # 清理测试文件
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
            s3_client.delete_object(Bucket=bucket_name, Key=f"thumbnails/{os.path.basename(s3_key)}")
            print("测试文件已清理")
        except Exception as e:
            print(f"清理文件时发生错误: {str(e)}")

if __name__ == "__main__":
    test_lambda_function() 