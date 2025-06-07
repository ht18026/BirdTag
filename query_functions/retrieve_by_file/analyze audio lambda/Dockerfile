FROM python:3.9-slim-bullseye

# Set working directory
WORKDIR /var/task

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsndfile1 ffmpeg libstdc++6 libgomp1 \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install numpy with version constraint
RUN pip install --upgrade pip \
 && pip install --no-cache-dir "numpy<2.0"

# Install lightweight audio processing libraries to avoid numba issues
RUN pip install --no-cache-dir \
    soundfile==0.12.1 \
    scipy==1.10.1 \
    tflite-runtime==2.14.0 \
    awslambdaric \
    boto3 \
 && python -c "from tflite_runtime.interpreter import Interpreter; print('TFLite OK')" \
 && python -c "import soundfile; import scipy.signal; print('Audio libs OK')"

# Copy application code
COPY analyze_audio_lambda.py ./

# Set environment variables
ENV _HANDLER=analyze_audio_lambda.lambda_handler

# Lambda Runtime Interface Client
ENTRYPOINT ["/usr/local/bin/python3", "-m", "awslambdaric"]
CMD ["analyze_audio_lambda.lambda_handler"]