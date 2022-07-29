from dotenv import load_dotenv
import os
import boto3
import json
import io

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')

s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

S3_BUCKET = os.getenv('BUCKET')


def get_s3_temp_filenames(path):

    def get_filenames(path):
        results = []
        bucket = s3.Bucket(S3_BUCKET)
        for filename in bucket.objects.filter(Prefix=path):
            results.append(filename.key)
        return results

    path_list = path.split("/")
    path_length = len(path_list)


    if path_length == 3:
        if path_list[1] == "weather":
            filenames = get_filenames(path=path)
            filenames.pop(0)
            if filenames == []:
                return False
            else:
                return filenames

        elif path_list[1] == "precipitation":
            filenames = get_filenames(path=path)
            filenames.pop(0)
            if filenames == []:
                return False
            else:
                return filenames

    elif path_length == 4:

        if path_list[2] == "taipei":
            filenames = get_filenames(path=path)
            filenames.pop(0)
            if filenames == []:
                return False
            else:
                return filenames

        elif path_list[2] == "taichung":
            filenames = get_filenames(path=path)
            filenames.pop(0)
            if filenames == []:
                return False
            else:
                return filenames

    else:

        return "Path Undefined"


def get_data_from_s3(filename):

    def get_json_from_s3(S3_BUCKET, key):
        try:
            obj = s3.Object(S3_BUCKET, key)
            data = obj.get()['Body'].read().decode('utf-8').splitlines()
            return data
        except:
            return None
    try:
        json_data = get_json_from_s3(S3_BUCKET, filename)
        stations = json.loads(json_data[0])
        return stations

    except Exception as e:
        print(e)
        return None


def move_temp_file(origin, destination):
    try:
        s3.Object(S3_BUCKET, f"{ destination }").copy_from(CopySource=f"{ S3_BUCKET }/{ origin }")
        print("Copy File Done")
        s3.Object(S3_BUCKET, f"{ origin }").delete()
        print("Delete File Done")
        return True
    except Exception as e:
        print(e)
        return False

def insert_parquet_to_s3(df, filename):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    filepath = f"parquet/{ filename }"
    data = s3.Object(S3_BUCKET, filepath)
    respone = data.put(Body=buffer.getvalue())
    return respone






