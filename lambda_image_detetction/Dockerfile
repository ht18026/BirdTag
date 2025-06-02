FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ./

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY lambda_function.py ./
COPY utils.py ./

CMD ["lambda_function.handler"]
