# imports
import os
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket, cloud_storage_upload_blob_from_file
from prefect_gcp import GcpCredentials
from datetime import timedelta
from prefect.tasks import task_input_hash

print("Setup Complete")

# read data from the web into Dataframe
@task(log_prints=True, name=["fetch-file"], retries=3)
# cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
def fetch(dataset_url: str) -> pd.DataFrame:
    filename, _ = urllib.request.urlretrieve(dataset_url)
    return filename


# Tweak DataFrame,
@task(log_prints=True, name="read-and-tweak-df")
def read_and_tweak(path: str, dataset_url: str) -> pd.DataFrame:
    if dataset_url.endswith(".parquet"):
        df = pd.read_parquet(path=path)
    elif dataset_url.endswith(".csv.gz"):
        df = pd.read_csv(path, engine="pyarrow", compression="gzip")
    else:
        df = pd.read_csv(path, engine="pyarrow")

    print(f"Number of rows: {df.shape[0]}")
    print(f"Columns Dtype: {df.dtypes}")
    return df


# Write DataFrame to a specific folder after tweaking the DataFrame
@task(log_prints=True, name="write-to-local-file")
def write_local(df: pd.DataFrame, year: int, dataset_file: str) -> Path:
    directory = Path(f"{year}")
    path_name = directory / f"{dataset_file}.parquet"
    try:
        os.makedirs(directory, exist_ok=True)
        df.to_parquet(path_name, compression="gzip")
    except OSError as error:
        print(error)
    return path_name


# https://app.prefect.cloud/account/975bd9ed-5aef-4c8a-a413-23073fef3acb/workspace/a711abba-f5ae-4b00-9315-34f92f089b77/blocks/catalog
# Upload local parquet file to GCS
@task(log_prints=True)
def write_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("prefect-gcs-block-ny-taxi")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    print("Loaded data to GCS...Hooray!")
    return


# child ETL function
@flow(log_prints=True, name="etl-web-to-gcs")
def etl_web_to_gcs(year: int, month: int) -> None:
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    # execution
    path_name = fetch(dataset_url)
    df_tweak = read_and_tweak(path_name, dataset_url)
    path = write_local(df_tweak, year, dataset_file)
    write_gcs(path)


# Parent flow ETL
@flow(log_prints=True, name="etl-parent-to-gcs")
def etl_parent_gcs_flow(
    year: int = 2019, months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for month in months:
        etl_web_to_gcs(year, month)


# run main
if __name__ == "__main__":
    year = 2019
    months = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1]

    etl_parent_gcs_flow(year, months)

# Source: https://prefecthq.github.io/prefect-gcp/
# prefect block register -m prefect_gcp
