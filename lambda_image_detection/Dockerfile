FROM public.ecr.aws/lambda/python:3.9
RUN yum install -y mesa-libGL
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
COPY lambda_function.py ./
CMD ["lambda_function.handler"]

