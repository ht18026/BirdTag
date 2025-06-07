FROM public.ecr.aws/lambda/python:3.9

# Install system dependencies for OpenCV
RUN yum install -y mesa-libGL

# Upgrade pip and install numpy with version constraint
RUN pip install --upgrade pip && \
    pip install --no-cache-dir "numpy<2.0" && \
    pip install --no-cache-dir \
        torch==2.2.2+cpu \
        torchvision==0.17.2+cpu \
        torchaudio==2.2.2+cpu \
        -f https://download.pytorch.org/whl/cpu/torch_stable.html && \
    pip install --no-cache-dir \
        ultralytics \
        opencv-python-headless==4.9.0.80 \
        supervision

# Copy application code
COPY analyze_image_lambda.py ./

# Set the Lambda handler
CMD ["analyze_image_lambda.lambda_handler"]