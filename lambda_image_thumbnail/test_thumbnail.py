import base64
from thumbnail import lambda_handler
import cv2
import numpy as np

def test_lambda_function():
    # Read test image
    test_image_path = "crows_4.jpg"  # Make sure the test image exists at this path
    with open(test_image_path, "rb") as image_file:
        # Convert image to base64
        image_data = base64.b64encode(image_file.read()).decode('utf-8')
    
    # Create mock Lambda event
    test_event = {
        'body': image_data
    }
    
    # Call Lambda function
    response = lambda_handler(test_event, None)
    
    # Check response
    if response['statusCode'] == 200:
        # Decode and save the returned base64 image data
        output_image_data = base64.b64decode(response['body'])
        with open("output_thumbnail.jpg", "wb") as output_file:
            output_file.write(output_image_data)
        print("Test successful! Thumbnail saved as output_thumbnail.jpg")
    else:
        print(f"Test failed: {response['body']}")

if __name__ == "__main__":
    test_lambda_function() 