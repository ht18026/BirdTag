FROM public.ecr.aws/lambda/python:3.9

# Install only necessary system dependencies for audio processing
RUN yum install -y \
    libsndfile \
    ffmpeg

# Upgrade pip and install numpy with version constraint
RUN pip install --upgrade pip && \
    pip install --no-cache-dir "numpy<2.0"

# Install only required Python packages
RUN pip install --no-cache-dir \
    librosa==0.10.1 \
    tflite-runtime==2.14.0

# Copy application code
COPY lambda_function.py ./
COPY utils.py ./

# Set the Lambda handler
CMD ["lambda_function.handler"]