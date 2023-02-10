# imports
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

print("Setup Complete")

# Download trip data from GCS
@task(log_prints=True, name="extract-from-gcs")
def extract_from_gcs(year: int, month: int) -> Path:
    gcs_path = f"fhv_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-gcs-2023")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"{year}/")
    return Path(f"{year}/{gcs_path}")


# Data cleaning example
@task(log_prints=True, name="transform")
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path=path)
    print(f"Number of rows: {df.shape[0]}")
    print(f"Columns Dtype: {df.dtypes}")
    return df


# Write DataFrame to BigQuery
def write_bq(df: pd.DataFrame, year: int) -> None:
    gcp_credentials_block = GcpCredentials.load("ny-taxi-gcp-creds")
    df.to_gbq(
        destination_table=f"ny_taxi.ny_taxi_tripdata_{year}",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


# ETL flow to load data to BigQuery
@flow(log_prints=True, name="etl-gcs-to-bq")
def etl_gcs_to_bq(year: int, month: int):

    # step-by-step execution
    path = extract_from_gcs(year, month)
    df = transform(path)
    write_bq(df, year)


# Parent flow ETL
@flow(log_prints=True, name="etl-parent-to-bq")
def etl_parent_bq_flow(
    year: int = 2019, months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for month in months:
        etl_gcs_to_bq(year, month)


# run main
if __name__ == "__main__":
    year = 2019
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    etl_parent_bq_flow(year, months)
