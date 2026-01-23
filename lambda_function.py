import os
import requests
import boto3
import json
from datetime import datetime


def download_from_api(file_name: str) -> str:
    base_url = "https://download.companieshouse.gov.uk"
    url = f"{base_url}/{file_name}"

    download_path = f"/tmp/{file_name}"

    with requests.get(url, stream=True) as r:
        with open(download_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return download_path


def upload_file_to_s3(client: object, file_path: str, bucket: str, zone: str):
    now = datetime.now()
    partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    file_name = os.path.basename(file_path)
    s3_key = f"{zone}/{partition_path}/{file_name}"
    print(f"Uploading to s3://{bucket}/{s3_key}")
    client.upload_file(file_path, bucket, s3_key)


def ingest_partition(partition: str):
    s3 = boto3.client("s3")
    today = datetime.today().strftime("%Y-%m-%d")
    zip_file = f"psc-snapshot-{today}_{partition}.zip"
    f_path = download_from_api(zip_file)
    upload_file_to_s3(s3, f_path, "companies-house-psc", "raw")
    os.remove(f_path)


def lambda_handler(event, context):
    partition = event.get("partition")
    ingest_partition(partition)
    msg = f"Partition ingested: {partition}"
    return {"statusCode": 200, "body": json.dumps(msg)}
