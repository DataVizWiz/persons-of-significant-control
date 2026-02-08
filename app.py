import os
import tempfile
import zipfile
import requests
import logging
import polars as pl
from multiprocessing import Pool, cpu_count
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)s: %(message)s"
)

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

def download_zip_to_df(partition: str) -> pl.DataFrame:
    zip_fname = create_partition_file(partition)
    
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        download_from_api(zip_fname, temp_dir)
        zip_path = os.path.join(temp_dir, zip_fname)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        txt_file = os.path.join(temp_dir, zip_fname.replace(".zip", ".txt"))
        df = pl.read_ndjson(txt_file)
    return df

def worker(partition):
    pid = os.getpid()
    logging.info(f"Worker {pid} starting partition: {partition}")
    df = download_zip_to_df(partition)
    logging.info(f"Worker {pid} finished partition: {partition}")
    return df

if __name__ == "__main__":
    num_workers = max(1, cpu_count() - 2)
    partitions = [f"{i}of31" for i in range(1, 5)]
    
    with Pool(processes=num_workers) as pool:
        dfs = pool.map(worker, partitions)
    
    logging.info("All partitions processed successfully")