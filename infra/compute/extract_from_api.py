import os
import requests
import boto3
import json
from datetime import datetime


def download_file(today: str) -> str:
    base_url = "https://download.companieshouse.gov.uk/en_pscdata.html"
    file_name = f"persons-with-significant-control-snapshot-{today}.zip"
    url = f"{base_url}/{file_name}"

    with requests.get(url, stream=True) as r:
        with open(file_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return file_name


def upload_file_to_s3(client: object, file_path: str, bucket: str, zone: str):
    now = datetime.now()
    partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    file_name = os.path.basename(file_path)
    s3_key = f"{zone}/{partition_path}/{file_name}"

    print(f"Uploading to s3://{bucket}/{s3_key}")
    client.upload_file(file_path, bucket, s3_key)


def lambda_handler(event, context):
    today = datetime.today().strftime("%Y-%m-%d")
    zip_file = download_file(today)
    s3 = boto3.client("s3")
    upload_file_to_s3(s3, zip_file, "bucket-9374923", "raw")
    os.remove(zip_file)
    return {
        "statusCode": 200,
        "body": json.dumps({"date_stamp": today, "zip_file": zip_file}),
    }
