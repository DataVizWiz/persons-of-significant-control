import os
import tempfile
import zipfile
import requests
import logging
import boto3
import polars as pl
from multiprocessing import Pool, cpu_count
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)s: %(message)s",
)

S3_BUCKET = "companies-house-psc"
S3_RAW_PREFIX = "raw"
S3_PROCESSED_PREFIX = "processed"

def create_partition_file(partition: str) -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    return f"psc-snapshot-{today}_{partition}.zip"


def download_from_api(file_name: str, temp_dir: str):
    base_url = "https://download.companieshouse.gov.uk"
    url = f"{base_url}/{file_name}"

    os.makedirs(temp_dir, exist_ok=True)
    download_path = os.path.join(temp_dir, file_name)

    with requests.get(url, stream=True) as r:
        with open(download_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


@staticmethod
def unzip_file(zip_fname: str, extract_to: str):
    zip_path = os.path.join(extract_to, zip_fname)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


@staticmethod
def unnest_struct_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    Recursively flatten all struct columns in a DataFrame.
    """
    exprs = []
    for col, dtype in df.schema.items():
        if isinstance(dtype, pl.Struct):
            # Unnest struct column
            exprs.append(pl.col(col).struct.unnest())
        else:
            exprs.append(pl.col(col))

    flat_lf = df.select(exprs)

    # If there are still struct columns, flatten again recursively
    if any(isinstance(dtype, pl.Struct) for dtype in flat_lf.schema.values()):
        return unnest_struct_cols(flat_lf)

    return flat_lf


@staticmethod
def explode_list_cols(df: pl.DataFrame) -> pl.DataFrame:
    for col, dtype in df.schema.items():
        if isinstance(dtype, pl.List):
            df = df.explode(col)
    return df


@staticmethod
def transform_df(json_df: pl.DataFrame) -> pl.DataFrame:
    flat_df = unnest_struct_cols(json_df)
    explode_df = explode_list_cols(flat_df)
    return explode_df


def upload_file_to_s3(file_dir: str, file_name: str, bucket: str, zone: str):
    now = datetime.now()
    s3_client = boto3.client("s3")
    s3_dir = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    s3_key = f"{zone}/{s3_dir}/{file_name}"
    s3_client.upload_file(os.path.join(file_dir, file_name), bucket, s3_key)
    logging.info(f"[->] {file_name} uploaded to s3://{bucket}/{s3_key}")


def transform_partition(partition: str) -> pl.DataFrame:
    zip_fname = create_partition_file(partition)
    txt_fname = zip_fname.replace(".zip", ".txt")

    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        logging.info(f"[->] Downloading {partition} to {temp_dir}")
        download_from_api(zip_fname, temp_dir)
        upload_file_to_s3(temp_dir, zip_fname, S3_BUCKET, S3_RAW_PREFIX)
        unzip_file(zip_fname, temp_dir)

        txt_path = os.path.join(temp_dir, txt_fname)
        json_df = pl.read_ndjson(txt_path)
        logging.info(f"[->] Transforming {partition}")
        transformed_df = transform_df(json_df)

    return transformed_df


def worker(partition):
    pid = os.getpid()
    logging.info(f"Starting worker {pid} for {partition}")
    transform_partition(partition)
    logging.info(f"Worker {pid} finished for {partition}")


if __name__ == "__main__":
    num_workers = max(1, cpu_count() - 2)
    partitions = [f"{i}of31" for i in range(1, 5)]

    with Pool(processes=num_workers) as pool:
        dfs = pool.map(worker, partitions)

    logging.info("All partitions processed successfully")
