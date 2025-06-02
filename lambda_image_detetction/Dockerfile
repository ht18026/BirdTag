FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ./

RUN yum install -y mesa-libGL

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    torch==2.2.2+cpu torchvision==0.17.2+cpu torchaudio==2.2.2+cpu \
    -f https://download.pytorch.org/whl/cpu/torch_stable.html && \
    pip install --no-cache-dir -r requirements.txt

COPY lambda_function.py ./
COPY utils.py ./

CMD ["lambda_function.handler"]

