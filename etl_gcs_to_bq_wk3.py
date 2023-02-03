# imports
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

print("Setup Complete")

# Download trip data from GCS
@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("prefect-gcs-2023")
    gcs_block.get_directory(
        from_path=gcs_path, local_path="./data/"
    )  # the .. indicates the folder level
    return Path(f"./data/{gcs_path}")


# Data cleaning example
@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"Pre: Missing passenger count: {df.passenger_count.isna().sum()}")
    df.fillna(value={"passenger_count": 0}, inplace=True)
    print(f"Post: Missing passenger count: {df.passenger_count.isna().sum()}")
    return df


# Write DataFrame to BigQuery
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("prefect-gcs-2023-creds")
    df.to_gbq(
        destination_table="prefect_de_2023_datasetid.yellow_ny_taxi_trips_table",
        project_id="dtc-de-2023",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


# Main ETL flow to load data to BigQuery
@flow(log_prints=True)
def etl_gcs_to_bq():
    color = "yellow"
    year = 2021
    month = 1

    # step-by-step execution
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


# run main
if __name__ == "__main__":
    etl_gcs_to_bq()
