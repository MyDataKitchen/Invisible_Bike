from dotenv import load_dotenv
import os
import boto3
import io
import pandas as pd

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('BUCKET')

s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def get_parquet(key):
    buffer = io.BytesIO()
    data = s3.Object(S3_BUCKET, key)
    data.download_fileobj(buffer)
    df = pd.read_parquet(buffer)
    return df

def get_dates_s3(path):
    dates = []
    bucket = s3.Bucket(S3_BUCKET)
    for filename in bucket.objects.filter(Prefix=path):
        dates.append(filename.key.split('/')[2].split('_')[0])
    dates.pop(0)
    return dates