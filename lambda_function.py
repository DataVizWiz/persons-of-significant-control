import os
import requests
import boto3
import json
import zipfile
import pandas as pd
from datetime import datetime


def download_from_api(file_name: str):
    base_url = "https://download.companieshouse.gov.uk"
    url = f"{base_url}/{file_name}"

    with requests.get(url, stream=True) as r:
        with open(file_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


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
    download_from_api(zip_file)

    with zipfile.ZipFile(zip_file, "r") as zf:
        zf.extractall(".")

    txt_file = zip_file.replace(".zip", ".txt")
    init_df = pd.read_json(txt_file, lines=True)
    normalized_df = pd.json_normalize(init_df["data"], sep="_")
    csv_file = zip_file.replace(".zip", ".csv")
    normalized_df.to_csv(csv_file, index=False)

    for f in [zip_file, txt_file, csv_file]:
        upload_file_to_s3(s3, f, "companies-house-psc", "raw")


def lambda_handler(event, context):
    partition = "1of31"
    ingest_partition(partition)
    msg = f"Partition ingested: {partition}"
    return {"statusCode": 200, "body": json.dumps(msg)}
