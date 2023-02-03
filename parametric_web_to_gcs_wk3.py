# imports
import os
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GCS
from datetime import timedelta
from prefect.tasks import task_input_hash

print("Setup Complete")

# read data from the web into Dataframe
@task(log_prints=True, name=["read-df"], retries=3)
# cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
def fetch(dataset_url: str) -> pd.DataFrame:
    filename, _ = urllib.request.urlretrieve(dataset_url)
    df = pd.read_parquet(filename)
    return df


# Tweak DataFrame,
@task(log_prints=True, name="tweak-df")
def tweak(df: pd.DataFrame) -> pd.DataFrame:
    df_tweak = df
    print(df_tweak.head(n=2))
    print(f"Columns Dtype: {df_tweak.dtypes}")
    print(f"No. of row: {df_tweak.shape[0]}")
    return df_tweak


# Write DataFrame to a specific folder after tweaking the DataFrame
@task(log_prints=True, name="write-to-local-file")
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    directory = Path(f"data/data/{color}")
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
    gcp_block = GcsBucket.load("prefect-gcs-block")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    print("Loaded data to GCS...Hooray!")
    return


# child ETL function
@flow(log_prints=True, name="etl-web-to-gcs")
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    )

    # execution
    df = fetch(dataset_url)
    df_tweak = tweak(df)
    path = write_local(df_tweak, color, dataset_file)
    write_gcs(path)


# Parent flow ETL
@flow(log_prints=True, name="etl-parent")
def etl_parent_flow(
    color: str = "yellow", year: int = 2021, months: list[int] = [1, 2]
):
    for month in months:
        etl_web_to_gcs(color, year, month)


# run main
if __name__ == "__main__":
    color = "green"
    year = 2019
    months = [4]

    etl_parent_flow(color, year, months)

# Source: https://prefecthq.github.io/prefect-gcp/
# prefect block register -m prefect_gcp
