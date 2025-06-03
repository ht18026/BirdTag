import os
import boto3

s3 = boto3.client("s3")
MODEL_PREFIX = "models/"
MODEL_ENV = os.environ.get("MODEL_NAME", "model.pt")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "birdtag-models-fit5225-g138")

def download_file_from_s3(bucket, key, download_path):
    s3.download_file(bucket, key, download_path)

def get_model_path():
    tmp_path = os.path.join("/tmp", MODEL_ENV)

    if not os.path.exists(tmp_path):
        print(f"Downloading model {MODEL_ENV} from s3://{BUCKET_NAME}/{MODEL_ENV}")
        s3.download_file(BUCKET_NAME, MODEL_PREFIX + MODEL_ENV, tmp_path)
    else:
        print("Model already cached in /tmp")

    return tmp_path
