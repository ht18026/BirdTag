import json
import base64
import tempfile
import os
import boto3

def lambda_handler(event, context):
    """
    专门处理图像文件的 Lambda 函数
    """
    
    try:
        # 获取文件数据
        file_content_b64 = event['file_content']
        filename = event['filename']
        content_type = event['content_type']
        
        # 解码文件内容
        file_content = base64.b64decode(file_content_b64)
        
        # 创建临时文件
        temp_path = create_temp_image_file(file_content, filename)
        
        try:
            # 执行图像分析
            detected_species = analyze_image_file(temp_path)
            
            return {
                'detected_species': detected_species,
                'file_type': 'image',
                'filename': filename,
                'message': f'Image analysis completed. Detected {len(detected_species)} species.'
            }
            
        finally:
            # 清理临时文件
            if os.path.exists(temp_path):
                os.remove(temp_path)
                
    except Exception as e:
        print(f"Image analysis error: {str(e)}")
        raise Exception(f"Image analysis failed: {str(e)}")

def create_temp_image_file(file_content, filename):
    """创建临时图像文件"""
    extension = os.path.splitext(filename)[1] or '.jpg'
    
    temp_fd, temp_path = tempfile.mkstemp(
        suffix=extension,
        dir='/tmp',
        prefix='image_'
    )
    
    try:
        with os.fdopen(temp_fd, 'wb') as temp_file:
            temp_file.write(file_content)
    except:
        os.close(temp_fd)
        raise
    
    return temp_path

def analyze_image_file(image_path):
    """
    图像分析核心逻辑
    """
    try:
        # TODO: 实现图像分析逻辑
        # 可以使用 TensorFlow、PyTorch 或其他图像识别模型
        
        # 示例：使用预训练的鸟类识别模型
        # from image_bird_classifier import classify_bird
        # detected_species = classify_bird(image_path)
        
        # 当前返回模拟数据
        print(f"Analyzing image: {image_path}")
        
        # 模拟检测结果
        detected_species = ["Common Robin", "Blue Jay"]
        
        return detected_species
        
    except Exception as e:
        print(f"Image classification error: {e}")
        raise